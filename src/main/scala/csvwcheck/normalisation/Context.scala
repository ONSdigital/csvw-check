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

package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{NormalisationContext, Normaliser, noWarnings, parseNodeAsText}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Context {
  private val csvwContextUri = "http://www.w3.org/ns/csvw"

  private val normalisers: Map[String, Normaliser] = Map(
    // Context Properties
    "@language" -> Utils.normaliseLanguageProperty(PropertyType.Context),
    "@base" -> Utils.normaliseUrlLinkProperty(PropertyType.Context),
  )

  def normaliseContext(propertyType: PropertyType.Value): Normaliser = context => context.node match {
    case contextArrayNode: ArrayNode =>
      contextArrayNode.elements().asScala.toArray match {
        case Array(csvwContextUrlNode: TextNode) => normaliseContext(propertyType)(context.withNode(csvwContextUrlNode))
        case Array(csvwContextUrlNode: TextNode, contextObjectNode: ObjectNode) if csvwContextUrlNode.asText() == csvwContextUri =>
          Utils.normaliseObjectNode(normalisers, context.toChild(contextObjectNode, "1"))
            .flatMap({
              case (objectNode, stringWarnings) =>
                val unexpectedFields = objectNode.fieldNames().asScala.filter(!normalisers.contains(_)).toArray
                if (unexpectedFields.nonEmpty) {
                  Left(context.makeError(s"Unexpected fields: ${unexpectedFields.mkString(", ")} found on @context."))
                } else {
                  for {
                    newBaseUrl <- objectNode.getMaybeNode("@base").map(parseNodeAsText(_)).getOrElse(Right(context.baseUrl))
                    newLang <- objectNode.getMaybeNode("@language").map(parseNodeAsText(_)).getOrElse(Right(context.language))
                  } yield (getStandardContextNode(newBaseUrl, newLang), stringWarnings, propertyType)
                }
            })
        case _ => Left(context.makeError(s"Unexpected @context value: ${contextArrayNode.toPrettyString}"))
      }
    case contextNode: TextNode if contextNode.asText == csvwContextUri =>
      Right((getStandardContextNode(context.baseUrl, context.language), noWarnings, propertyType))
    case contextNode => Left(context.makeError(s"Unexpected @context value: ${contextNode.toPrettyString}"))
  }

  def getBaseUrlAndLanguageFromContext(rootNodeContext: NormalisationContext[ObjectNode]): ParseResult[(String, String)] = {
    rootNodeContext.node
      .getMaybeNode("@context")
      .map(contextNode =>
        normaliseContext(PropertyType.Context)(rootNodeContext.toChild(contextNode, "@context"))
          .map({
            case (parsedContextNode, _, _) =>
              val contextObjectNode = parsedContextNode.get(1)
              (
                contextObjectNode.get("@base").asText,
                contextObjectNode.get("@language").asText
              )
          })
      )
      // The top-level object of a metadata document or object referenced through an object property
      // (whether it is a table group description, table description, schema, dialect description or
      // transformation definition) MUST have a @context property.
      .getOrElse(Left(rootNodeContext.makeError(s"A @context node must be defined.")))
  }

  private def getStandardContextNode(baseUrl: String, language: String) = {
    val contextArrayNode = JsonNodeFactory.instance.arrayNode()
    val contextObjectNode = JsonNodeFactory.instance.objectNode()

    contextObjectNode.set("@base", new TextNode(baseUrl))
    contextObjectNode.set("@language", new TextNode(language))

    contextArrayNode.add(new TextNode(csvwContextUri))
    contextArrayNode.add(contextObjectNode)

    contextArrayNode
  }
}
