package csvwcheck.standardisers

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.standardisers.Utils.{JsonNodeParser, parseNodeAsText}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Context {
  private val csvwContextUri = "http://www.w3.org/ns/csvw"

  private val parsers: Map[String, JsonNodeParser] = Map(
    // Context Properties
    "@language" -> Utils.parseLanguageProperty(PropertyType.Context),
    "@base" -> Utils.parseUrlLinkProperty(PropertyType.Context),
  )

  def parseContext(propertyType: PropertyType.Value): JsonNodeParser = {
    case (contextArrayNode: ArrayNode, baseUrl, lang) =>
      contextArrayNode.elements().asScala.toArray match {
        case Array(csvwContextUrlNode: TextNode) => parseContext(propertyType)(csvwContextUrlNode, baseUrl, lang)
        case Array(csvwContextUrlNode: TextNode, contextObjectNode: ObjectNode) if csvwContextUrlNode.asText() == csvwContextUri =>
          Utils.parseObjectNode(contextObjectNode, parsers, baseUrl, lang)
            .flatMap({
              case (objectNode, stringWarnings) =>
                for {
                  newBaseUrl <- objectNode.getMaybeNode("@base").map(parseNodeAsText(_)).getOrElse(Right(baseUrl))
                  newLang <- objectNode.getMaybeNode("@language").map(parseNodeAsText(_)).getOrElse(Right(lang))
                } yield (getStandardContextNode(newBaseUrl, newLang), stringWarnings, propertyType)
            })
        case _ => Left(MetadataError(s"Unexpected @context value: ${contextArrayNode.toPrettyString}"))
      }
    case (contextNode: TextNode, baseUrl, lang) if contextNode.asText == csvwContextUri =>
      Right((getStandardContextNode(baseUrl, lang), Array[String](), propertyType))
    case (contextNode, _, _) => Left(MetadataError(s"Unexpected @context value: ${contextNode.toPrettyString}"))
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
