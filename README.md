# Query Converter

The `QueryConverter.py` script converts team-level CxSAST query
overrides to projet-level equivalents. In the case where a query is
already overridden at the project level, or there are multiple
overrides at different levels in the team hierarchy, the queries are
merged.

## Usage

Invoking the `QueryConverter.py` script with the `-h` or `--help`
command line options generates a usage message:

```
usage: QueryConverter.py [-h] [--debug] [--dry-run] [--pretty-print] [-p PROJECT]

Convert team-level CxSAST queries to project-level queries

options:
  -h, --help            show this help message and exit
  --debug               Enable debug output
  --dry-run             Enable dry run mode (no changes are made to the CxSAST instance)
  --pretty-print        Pretty print the old and new query groups
  -p PROJECT, --project PROJECT
                        Only modify queries for the specified project (this option may be provided multiple times)
```
