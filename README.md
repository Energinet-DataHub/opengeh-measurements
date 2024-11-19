# Measurements

## Folder Structure

This repository follows structure defined by [ADR-102](https://energinet.atlassian.net/wiki/spaces/D3/pages/290390042/ADR+102+-+Folderstruktur).

``` txt
<root>
├───.github
├───docs
├───source
├───.gitignore
├───.editorconfig
├───.gitignore
├───README.md
└───wordlist.txt
```

### `root`

Contains:

- `.gitignore` file that defines which files should be ignored (not checked in) by Git.
- `.editorconfig` file that defines coding styles for different editors and IDEs.
- `README.md` file that gives an introduction to this repository.
- `wordlist.txt` file used for markdown spell checking.

### `docs`

Contains notes and documentation stored in `*.md` files.

### `source`

Contains source code.
