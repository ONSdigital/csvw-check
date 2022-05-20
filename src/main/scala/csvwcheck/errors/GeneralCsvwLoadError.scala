package csvwcheck.errors

/**
  * Any kind of exception which occurs when loading a CSV-W schema file which is fatal and should result in immediate
  * termination of the validation attempt.
  *
  * @param error - The error causing the CSV-W to fail to load.
  */
case class GeneralCsvwLoadError(error: Throwable) extends CsvwLoadError
