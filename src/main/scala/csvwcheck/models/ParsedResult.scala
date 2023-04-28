package csvwcheck.models

/**
  * Holds a parsed component together with its warnings and errors.
  * @param component
  * @param warningsAndErrors
  * @tparam T
  */
case class ParsedResult[T](component: T, warningsAndErrors: WarningsAndErrors)

