package csvwcheck.standardisers

import csvwcheck.enums.PropertyType
import csvwcheck.standardisers.Utils.JsonNodeParser

object IdProperty {
  val parser: Map[String, JsonNodeParser] = Map(
    "@id" -> Utils.parseUrlLinkProperty(PropertyType.Common),
  )
}
