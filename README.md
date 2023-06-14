# csvw-check

Validate CSV-W based on tests provided by W3C (https://w3c.github.io/csvw/tests/#manifest-validation)

## Using csvw-check

### Help

```bash
$ csvw-check --help
csvw-check 0.0.1
Usage: csvw-check [options]

  -s, --schema <value>     filename of Schema/metadata file
  -c, --csv <value>        filename of CSV file
  -l, --log-level <value>  OFF|ERROR|WARN|INFO|DEBUG|TRACE
  -h, --help               prints this usage text
```

### Docker

```bash
$ docker pull gsscogs/csvw-check:latest
$ docker run --rm gsscogs/csvw-check:latest bin/csvw-check -s https://w3c.github.io/csvw/tests/test011/tree-ops.csv-metadata.json
Valid CSV-W
```

### Not Docker

Acquire the latest universal 'binary' ZIP file from the releases tab (e.g. [csvw-check-0.0.3.zip](https://github.com/GSS-Cogs/csvw-check/releases/download/v0.0.3/csvw-check-0.0.3.zip)).

```bash
$ bin/csvwcheck -s https://w3c.github.io/csvw/tests/test011/tree-ops.csv-metadata.json
Valid CSV-W
```
