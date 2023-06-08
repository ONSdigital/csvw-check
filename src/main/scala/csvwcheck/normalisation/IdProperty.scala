package csvwcheck.normalisation

import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.Utils.Normaliser

object IdProperty {
  val normaliser: Map[String, Normaliser] = Map(
    "@id" -> Utils.normaliseUrlLinkProperty(PropertyType.Common),
  )
}
