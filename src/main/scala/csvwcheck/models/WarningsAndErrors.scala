package csvwcheck.models

import csvwcheck.errors.{ErrorWithCsvContext, WarningWithCsvContext}

object WarningsAndErrors {
  type Warnings = Array[WarningWithCsvContext]
  type TolerableErrors = Array[ErrorWithCsvContext]
}

case class WarningsAndErrors(
                              warnings: WarningsAndErrors.Warnings = Array(),
                              errors: WarningsAndErrors.TolerableErrors = Array()
                            )
