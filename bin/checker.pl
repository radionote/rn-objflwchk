# http://radionote.blogspot.com  2008-12-31

use strict;
use IO::Dir;
use Sys::Hostname;
use IO::File;
use File::Spec;

##### global vals

# node identifier of this node
my $this_node;
# to which this node is assigned
my $checkpoint;
# before this checkpoint, objects must be checked by these checkpoints
my @checkpoints_before_this_checkpoints;
# checkpoint record cache
my %checkpoints_records_last_read;
my %checkpoints_records;

##### main

&main();
exit( 0 );

sub main {
    $this_node = &this_node_id();
    print "Executing $0 on $this_node at ",&time_down_to_second(),".\n";

    $checkpoint = &which_checkpoint_this_node_assigned_to();
    print "This node is assigned to checkpoint $checkpoint.\n";

    my %cps = &read_definition_of_checkpoints_file();
    my $cpsb = $cps{$checkpoint};
    @checkpoints_before_this_checkpoints = @$cpsb;
    print "Before this checkpoint, objects must be checked by the following ", ( $#checkpoints_before_this_checkpoints + 1 ), " checkpoints: ", join( ', ', @checkpoints_before_this_checkpoints ), ".\n";

    foreach ( @checkpoints_before_this_checkpoints ) {
	&read_checkpoints_records_files( $_ );
    }
    &read_checkpoints_records_files( $checkpoint );

    &print_help();

    while () {
	print "$checkpoint> ";
	my $inp = <STDIN>;
	defined( $inp ) or next;
	chomp( $inp );

	my ( $cmd, $oid ) = &parse_command( $inp );

	if ( $cmd eq 'exit' ) {
	    print "Exiting.\n";
	    last;
	}

	if ( $cmd eq 'help' ) {
	    &print_help();
	    next;
	}

	if ( ! &is_acceptable_object_identifier( $oid ) ) {
	    print "$oid contains unacceptable characters.\n";
	    next;
	}  

	if ( $cmd eq 'cancel-checking' ) {
	    if ( ! &object_was_checked_by_checkpoint( $oid, $checkpoint ) ) {
		print "Object [$oid] has not been checked.\n";
		next;
	    }
	    &cancel_checking_object( $oid );
	    print "Canceled checking object [$oid].\n";
	    next;
	}

	if ( $cmd eq 'check' ) {
	    my $before = &object_was_checked_by_checkpoint( $oid, $checkpoint );
	    if ( $before ) {
		print "Object [$oid] has been checked already at ",( split( /\s+/, $before ) )[0],".\n";
		next;
	    }
	    my $cpb = &is_object_rejected( $oid );
	    if ( $cpb ) {
		print "Rejected checking object [$oid]. It has not been checked by checkpoint $cpb.\n";
		next;
	    }
	    &check_object( $oid );
	    print "Checked object [$oid].\n";
	    next;
	}

	die 'Bug';
    }
}

##### main end

##### user interface

sub parse_command {
    my ( $cmd ) = @_;

    if ( $cmd =~ /^([^\s]+)(\s)(.+)$/ || $cmd =~ /^([^\s]+)$/ ) {
	$1 eq 'exit' and return 'exit';
	$1 eq 'help' and return 'help';
	$1 eq 'c' || $1 eq 'cancel-checking' and return ( 'cancel-checking', $3 );
	$1 eq 'check' and return ( 'check', $3 );
	#print "Command $1 not found. Assumed `check $cmd'.\n";
    }
    return ( 'check', $cmd );
}

sub print_help {
    print "Enter command after `>'.
To check object whose identifier is `1234567890',
  $checkpoint> 1234567890
  $checkpoint> check 1234567890
To cancel checking object whose identifier is `1234567890',
  $checkpoint> c 1234567890
  $checkpoint> cancel-checking 1234567890
To show this instruction,
  $checkpoint> help
To exit this program,
  $checkpoint> exit
";
}

##### checkpoints records

sub check_object {
    my ( $oid ) = @_;
    &check_cancel_object( $oid, 'checked' );
}

sub cancel_checking_object {
    my ( $oid ) = @_;
    &check_cancel_object( $oid, 'canceled-checking' );
}

sub check_cancel_object {
    my ( $oid, $cc ) = @_;

    my $file = File::Spec->catfile(
	File::Spec->splitdir( &records_at_checkpoint_dir( $checkpoint ) ),
	&time_down_to_hour().'.checked' );

    my $fh = IO::File->new( $file, O_WRONLY|O_CREAT|O_APPEND )
	or die "Can not open file of records at a checkpoint: $file";
    print $fh &time_down_to_second(),"\t",$this_node,"\t",$cc,"\t",$oid,"\n";
    undef( $fh );    
}

sub is_object_rejected {
    my ( $oid ) = @_;
    my $cpb;
    foreach $cpb ( @checkpoints_before_this_checkpoints ) {
	&object_was_checked_by_checkpoint( $oid, $cpb ) and next;
	return $cpb;
    }
}

sub object_was_checked_by_checkpoint {
    my ( $oid, $cpb ) = @_;

    my @records = sort { $a cmp $b } grep { /^\S+\s+\S+\s+$oid$/ } &read_checkpoints_records_files( $cpb );
    #print $#records , "+1 records were matched for object $oid.\n"; ##
    if ( $#records >= 0 ) {
	my $lastrecord = pop( @records );
	$lastrecord =~ /^\S+\s+(checked|canceled\-checking)\s+$oid$/
	    or die "checkpoint record contains a line in unknown format: $lastrecord";
	$1 eq 'checked' and return $lastrecord;
    }
    return;
}

sub read_checkpoints_records_file {
    my ( $file ) = @_;

    -f $file && -r $file or die "Can not read file of records at a checkpoint: $file";

    my $mtime = ( stat( $file ) )[9];
    if ( defined( $checkpoints_records_last_read{$file} )
	 && $mtime < $checkpoints_records_last_read{$file} ) {
	#print "File $file does not change after last reading.\n";
	return $checkpoints_records{$file};
    }

    print "Reading file $file...";

    $checkpoints_records_last_read{$file} = time();

    my @contents;
    my $fh = IO::File->new( $file, 'r' )
	or die "Can not open file of records at a checkpoint: $file";
    my $line;
    while ( $line = <$fh> ) {
	chomp( $line );
	my ( $time, $node, $cc, $objid ) = split( /\s+/, $line );
	if ( ! defined( $objid ) ) {
	    warn "file of records at a checkpoint $file contains line in unknown format: $line";
	    next;
	}
	push( @contents, join( ' ', ( $time, $cc, $objid ) ) );
    }
    undef( $fh );
    $checkpoints_records{$file} = \@contents;

    print "Done.\n";
    
    return $checkpoints_records{$file};
}

sub read_checkpoints_records_files {
    my ( $cp ) = @_;

    my $d = &records_at_checkpoint_dir( $cp );
    my $dir = IO::Dir->new( $d )
	or die "Can not open directory $d";
    my @files = sort { $a cmp $b } grep { /^.*\.checked$/i } $dir->read();
    undef( $dir );

    my @records;
    foreach ( @files ) {
	my $file = File::Spec->catfile(
	    File::Spec->splitdir( $d ),
	    $_ );
	-f $file or next;
	my $rec = &read_checkpoints_records_file( $file );
	push( @records, @$rec );
    }
    #print $#records, "+1 records were read for checkpoint $cp.\n"; ##
    return @records;
}

##### time

sub time_down_to_second {
    my ( $sec,$min,$hh,$dd,$mm,$yy ) = gmtime();

    $yy += 1900;
    $mm = &head_0( $mm + 1 );
    $dd = &head_0( $dd );
    $hh = &head_0( $hh );
    $min = &head_0( $min );
    $sec = &head_0( $sec );
    
    return "$yy-$mm-$dd".'T'."$hh$min$sec".'Z';
}

sub time_down_to_hour {
    my ( $sec,$min,$hh,$dd,$mm,$yy ) = gmtime();

    $yy += 1900;
    $mm = &head_0( $mm + 1 );
    $dd = &head_0( $dd );
    $hh = &head_0( $hh );
    
    return "$yy-$mm-$dd".'T'."$hh".'Z';
}

sub head_0 {
    my ( $str ) = @_;

    $str = '00' . $str;
    $str =~ /(\d{2})$/;
    return $1;
}

##### node assignment

sub assign_this_node_to_checkpoint {
    my ( $cp ) = @_;

    my $file = &node_assigned_for_checkpoint_file( $cp );
    -e $file and die "Already exists: $file";

    my $fh = IO::File->new( $file, O_WRONLY|O_CREAT )
	or die "Can not open file of assigned node for checkpoint $cp: $file. Parent directory may not exist.";
    print $fh $this_node;
    print $fh "\n",&time_down_to_second();
    undef( $fh );

    return $cp;
}

sub select_checkpoint_this_node_assigned {
    my ( @cpsnons ) = @_;
    
    print "Select from the following checkpoints to which this node will be assigned:\n";
    foreach ( @cpsnons ) {
	print "  ";
	print ;
	print "\n";
    }

    print "> ";
    my $cp = <STDIN>;
    defined( $cp ) or return &select_checkpoint_this_node_assigned( @cpsnons );
    chomp( $cp );
    if ( ! &is_acceptable_checkpoint_name( $cp ) ) {
	print "Checkpoint $cp contains unacceptable characters.\n";
	return &select_checkpoint_this_node_assigned( @cpsnons );
    }
    foreach ( @cpsnons ) {
	if ( $_ eq $cp ) {
	    if ( &assign_this_node_to_checkpoint( $cp ) ) {
		return $cp;
	    } else {
		return &select_checkpoint_this_node_assigned( @cpsnons );
	    }
	}
    }

    return &select_checkpoint_this_node_assigned( @cpsnons );
}

sub which_checkpoint_this_node_assigned_to {
    my $thiscp;
    my @cpsnonodes;

    my %cps = &read_definition_of_checkpoints_file();
    my $cp;
    foreach $cp ( keys( %cps ) ) {
	my $nodid = ( &node_assigned_for_checkpoint( $cp ) )[1];
	if ( ! defined( $nodid ) ) {
	    # warn "No node assigned for checkpoint $cp";
	    push( @cpsnonodes, $cp );
	    next;
	}
	if ( $this_node eq $nodid ) {
	    defined( $thiscp ) and die "This node is assigned to both of checkpoints $thiscp and $cp.";
	    $thiscp = $cp;
	}
    }
    defined( $thiscp ) and return $thiscp;

    print "This node $this_node has not been assigned to any checkpoints yet.\n";
    $#cpsnonodes < 0 and die "All checkpoints have had nodes assigned to them.";
    return &select_checkpoint_this_node_assigned( @cpsnonodes );
}

sub node_assigned_for_checkpoint {
    my ( $cp ) = @_;

    my $file = &node_assigned_for_checkpoint_file( $cp );
    -e $file or return;

    my @na = ( $cp );
    my $fh = IO::File->new( $file, 'r' )
	or die "Can not open the file of assigned node for checkpoint $cp: $file";
    while ( <$fh> ) {
	chomp();
	push( @na, $_ );
    }
    undef( $fh );

    return @na;
}

sub this_node_id {
    my $user = $ENV{USER};
    defined( $user ) or $user = $ENV{USERNAME};
    return $user.'@'.Sys::Hostname->hostname();
}

##### definition of checkpoints

sub read_definition_of_checkpoints_file {
    my %cps;

    my $fh = &open_definition_of_checkpoints_file();
    my $line;
    while ( $line = <$fh> ) {
	chomp( $line );
	$line =~ /^\s*$/ and next;
	$line =~ /^\s*#/ and next;
	my ( $cp, $cpsbefore ) = split( /\s+/, $line );
	&is_acceptable_checkpoint_name( $cp )
	    or die "File of definition of checkpoints contains unacceptable name of checkpoint: $cp";
	defined( $cps{$cp} )
	    and die "File of definition of checkpoints contains duplicated checkpoints: $cp";
	my @cpsbefore;
	defined( $cpsbefore ) and @cpsbefore = split( /;/, $cpsbefore );
	$cps{$cp} = \@cpsbefore;
    }
    undef( $fh );

    my $cpsbefore;
    foreach $cpsbefore ( values( %cps ) ) {
	foreach ( @$cpsbefore ) {
	    &is_acceptable_checkpoint_name( $_ )
		or die "File of definition of checkpoints contains unacceptable name of checkpoint: $_";
	    defined( $cps{$_} )
		or die "File of definition of checkpoints contains an inconsistency: a checkpoint after $_, $_ not defined";
	}
    }

    return %cps;
}

sub open_definition_of_checkpoints_file {
    return IO::File->new( &definition_of_checkpoints_file(), 'r' )
	or die "Can not open the file of definition of checkpoints";
}

##### inventory

sub inventory_dir {
    my @dirs = File::Spec->splitdir( $0 );
    pop( @dirs );
    pop( @dirs );
    push( @dirs, 'inventory' );
    return File::Spec->catdir( @dirs );
}

sub inventory_conf_dir {
    my @dirs = File::Spec->splitdir( &inventory_dir() );
    push( @dirs, 'conf' );
    return File::Spec->catdir( @dirs );
}

sub definition_of_checkpoints_file {
    my $file = File::Spec->catfile(
	File::Spec->splitdir( &inventory_conf_dir() ),
	'checkpoints.definition' );
    -r $file or die "Can not read $file";
    -T $file or die "Does not text file: $file";
    return $file;
}

sub records_at_checkpoints_superdir {
    my @dirs = File::Spec->splitdir( &inventory_dir() );
    push( @dirs, 'checkpoints-records' );
    return File::Spec->catdir( @dirs );
}

sub records_at_checkpoint_dir {
    my ( $cp ) = @_;
    &is_acceptable_checkpoint_name( $cp )
	or die "Unacceptable checkpoint name: $cp";
    my @dirs = File::Spec->splitdir( &records_at_checkpoints_superdir() );
    push( @dirs, $cp );
    return File::Spec->catdir( @dirs );    
}

sub node_assigned_for_checkpoint_file {
    my ( $cp ) = @_;
    my $file = File::Spec->catfile(
	File::Spec->splitdir( &records_at_checkpoint_dir( $cp ) ),
	'node.assigned' );
    if ( -e $file ) {
	-r $file or die "Can not read $file";
	-T $file or die "Does not text file: $file";
    }
    return $file;
}

#####

sub is_acceptable_object_identifier {
    my ( $oid ) = @_;
    return ( $oid =~ /^[^\n\r\f]+$/ );
}

sub is_acceptable_checkpoint_name {
    my ( $cp ) = @_;
    return ( $cp =~ /^[^\t\n\r\f;]+$/ );
}
