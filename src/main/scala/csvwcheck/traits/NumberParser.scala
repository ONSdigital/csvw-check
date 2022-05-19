package csvwcheck.traits

trait NumberParser {
  def parse(number: String): Either[String, BigDecimal]
}
