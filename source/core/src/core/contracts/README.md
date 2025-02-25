﻿# Contracts

This directory contains contracts.

## Data products

The contracts are written as data products in the form of a Python file that contains the Structured Data of the
contract.

## Protobuf

In this folder we have protobuf files and the `assets` folder contains the descriptor files.

We are using the compiled protobuf files in the code which means that it is important that the descriptor files
are always up to date. There is a `test_descriptor_files.py` which tests exactly this.

### Compile protobuf files

If we for some reason need to make changes in the protobuf files and then need to compile them, we have made it
easy to do so right [here](./process_manager/scripts/compile_protobuf.py).

If new protobuf files are added, they should be added to the script as well.
