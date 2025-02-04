# Contract

## Protobuf

In this folder we have protobuf files and the `assets` folder contains the descriptor files.

We are using the compiled protobuf files in the code which means that it is important that the descriptor files
are always up to date. There is a `test_descriptor_files.py` which tests exactly this.

### Compile protobuf files

If we for some reason needs to make changes in the protobuf files and then need to compile them, we have made it
easy to do so right [here](./scripts/compile_protobuf.py).

If new protobuf files are being added, it should be added to the script as well.
