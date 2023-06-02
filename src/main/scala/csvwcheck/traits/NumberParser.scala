package csvwcheck.traits

import csvwcheck.models.ParseResult.ParseResult

trait NumberParser {
  def parse(number: String): ParseResult[BigDecimal]
}
