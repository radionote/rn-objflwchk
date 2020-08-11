# Object Flow Checkpoint System

## Requirements

- Perl modules
    - `IO::Dir`
    - `Sys::Hostname`
    - `IO::File`
    - `File::Spec`

## Environments

This checker program was observed working on the following environments.

- Perl 5.8.8, Mac OS X 10.5.5 Intel
- [Strawberry Perl](http://strawberryperl.com) 5.10.0.2, Windows XP Professional SP3, by assigning a network drive for the shared directory including the inventory and the checker program. It did not work when the inventory and the checker pragram were accessed by paths like `\\fileserver\rn-objflwchk\bin\checker.bat`
