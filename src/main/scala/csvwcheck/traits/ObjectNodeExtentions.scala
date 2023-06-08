package csvwcheck.traits

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import csvwcheck.normalisation.Utils.NormContext

object ObjectNodeExtentions {
  implicit class IteratorHasGetKeysAndValues(objectNode: ObjectNode) {

    /** Get the (key, value) pairs from a Jackson ObjectNode **/
    def getKeysAndValues: Array[(String, JsonNode)] =
      JavaIteratorExtensions
        .IteratorHasAsScalaArray(objectNode.fields())
        .asScalaArray
        .map(kvp => (kvp.getKey, kvp.getValue))

    def normaliseChildren(context: NormContext[ObjectNode]): Array[NormContext[JsonNode]] =
      objectNode
        .getKeysAndValues
        .map({case (propertyName, valueNode) => context.toChild(valueNode, propertyName)})
  }

  implicit class ObjectNodeGetMaybeNode(objectNode: ObjectNode) {

    /** Get the (key, value) pairs from a Jackson ObjectNode **/
    def getMaybeNode(propertyName: String): Option[JsonNode] = {
      val maybeNode = objectNode.path(propertyName)
      if (maybeNode.isMissingNode || maybeNode.isNull) {
        None
      } else {
        Some(maybeNode)
      }
    }
  }

}
