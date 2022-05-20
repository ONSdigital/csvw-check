package csvwcheck.errors

case class SchemaDoesNotContainCsvError(error: Throwable) extends CsvwLoadError
