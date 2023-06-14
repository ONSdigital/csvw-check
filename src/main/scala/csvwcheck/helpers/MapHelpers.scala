package csvwcheck.helpers

import com.fasterxml.jackson.databind.JsonNode

import scala.collection.mutable

object MapHelpers {
  def deepCloneJsonPropertiesMap(
                                  propertiesMap: mutable.Map[String, JsonNode]
                                ): mutable.Map[String, JsonNode] = {
    mutable.Map.from(
      propertiesMap.toArray.map({
        case (propertyName: String, value: JsonNode) =>
          (propertyName, value.deepCopy())
      })
    )
  }
}
