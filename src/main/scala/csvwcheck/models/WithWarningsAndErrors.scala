package csvwcheck.models

/**
  * Holds a parsed component together with its warnings and errors.
  * @param component
  * @param warningsAndErrors
  * @tparam T
  */
case class WithWarningsAndErrors[T](
    component: T,
    warningsAndErrors: WarningsAndErrors
)
