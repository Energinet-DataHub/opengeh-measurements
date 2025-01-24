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

## Getting started

Prerequisites: Docker Desktop and VSCode

- Open VSCode
- Install extension "Dev Containers"
- Press F1, find and select "Dev Containers: Rebuild and Reopen in Container"
- Select the container you want to build, e.g. "capacity-settlement"
    - *If the build fails try removing the folder `.uv_cache` in the repo root folder
- To switch between containers press F1 and select "Dev Containers: Switch Container"
- Open the "Testing" panel to the left to discover and run tests
