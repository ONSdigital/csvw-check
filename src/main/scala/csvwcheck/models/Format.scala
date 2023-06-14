package csvwcheck.models

case class Format(
                   pattern: Option[String],
                   decimalChar: Option[Char],
                   groupChar: Option[Char]
                 )
