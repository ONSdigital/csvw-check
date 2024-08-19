/*
 * Copyright 2020 Crown Copyright (Office for National Statistics)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package csvwcheck.traits

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import csvwcheck.normalisation.Utils.NormalisationContext

object ObjectNodeExtentions {
  implicit class IteratorHasGetKeysAndValues(objectNode: ObjectNode) {

    def normaliseChildren(context: NormalisationContext[ObjectNode]): Array[NormalisationContext[JsonNode]] =
      objectNode
        .getKeysAndValues
        .map({ case (propertyName, valueNode) => context.toChild(valueNode, propertyName) })

    /** Get the (key, value) pairs from a Jackson ObjectNode * */
    def getKeysAndValues: Array[(String, JsonNode)] =
      JavaIteratorExtensions
        .IteratorHasAsScalaArray(objectNode.fields())
        .asScalaArray
        .map(kvp => (kvp.getKey, kvp.getValue))
  }

  implicit class ObjectNodeGetMaybeNode(objectNode: ObjectNode) {

    /** Get the (key, value) pairs from a Jackson ObjectNode * */
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
