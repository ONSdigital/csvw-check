package csvwcheck.errors

/**
  * An specific type of error occurred which means we should continue searching for a matching CSV-W schema file in the
  * conventional locations.
  *
  * @param error - The underlying error meaning this CSV-W cannot be loaded.
  */
case class CascadeToOtherFilesError(error: Throwable) extends CsvwLoadError
