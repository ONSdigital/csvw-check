# csvw-check

Validate CSV-W based on tests provided by W3C (https://w3c.github.io/csvw/tests/#manifest-validation)

## Using csvw-check

### Help

```bash
$ csvwcheck --help
CSVW-Validation 1.0
Usage: CSVW-Validation [options]

  -s, --schema <value>     filename of Schema/metadata file
  -c, --csv <value>        filename of CSV file
  -l, --log-level <value>  OFF|ERROR|WARN|INFO|DEBUG|TRACE
```

### Docker

```bash
$ docker pull gsscogs/csvw-check:latest
$ docker run --rm gsscogs/csvw-check:latest bin/csvwcheck -s https://w3c.github.io/csvw/tests/test011/tree-ops.csv-metadata.json
Valid CSV-W
```

### Not Docker

Acquire the latest universal 'binary' ZIP file from the releases tab (e.g. [csvwcheck-0.0.2.zip](https://github.com/GSS-Cogs/csvw-check/releases/download/v0.0.2/csvwcheck-0.0.2.zip)).

```bash
$ bin/csvwcheck -s https://w3c.github.io/csvw/tests/test011/tree-ops.csv-metadata.json
Valid CSV-W
```
