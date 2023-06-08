package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{Normaliser, noWarnings, parseNodeAsText}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Context {
  private val csvwContextUri = "http://www.w3.org/ns/csvw"

  private val normalisers: Map[String, Normaliser] = Map(
    // Context Properties
    "@language" -> Utils.normaliseLanguageProperty(PropertyType.Context),
    "@base" -> Utils.normaliseUrlLinkProperty(PropertyType.Context),
  )

  def normaliseContext(propertyType: PropertyType.Value): Normaliser = {
    case (contextArrayNode: ArrayNode, baseUrl, lang, propertyPath) =>
      contextArrayNode.elements().asScala.toArray match {
        case Array(csvwContextUrlNode: TextNode) => normaliseContext(propertyType)(csvwContextUrlNode, baseUrl, lang, propertyPath)
        case Array(csvwContextUrlNode: TextNode, contextObjectNode: ObjectNode) if csvwContextUrlNode.asText() == csvwContextUri =>
          Utils.normaliseObjectNode(contextObjectNode, normalisers, baseUrl, lang, propertyPath :+ "1")
            .flatMap({
              case (objectNode, stringWarnings) =>
                for {
                  newBaseUrl <- objectNode.getMaybeNode("@base").map(parseNodeAsText(_)).getOrElse(Right(baseUrl))
                  newLang <- objectNode.getMaybeNode("@language").map(parseNodeAsText(_)).getOrElse(Right(lang))
                } yield (getStandardContextNode(newBaseUrl, newLang), stringWarnings, propertyType)
            })
        case _ => Left(MetadataError(s"Unexpected @context value: ${contextArrayNode.toPrettyString}", propertyPath))
      }
    case (contextNode: TextNode, baseUrl, lang, _) if contextNode.asText == csvwContextUri =>
      Right((getStandardContextNode(baseUrl, lang), noWarnings, propertyType))
    case (contextNode, _, _, propertyPath) => Left(MetadataError(s"Unexpected @context value: ${contextNode.toPrettyString}", propertyPath))
  }

  def getBaseUrlAndLanguageFromContext(initialBaseUrl: String, initialLang: String, documentRootNode: ObjectNode): ParseResult[(String, String)] = {
    documentRootNode
      .getMaybeNode("@context")
      .map(contextNode =>
        normaliseContext(PropertyType.Context)(contextNode, initialBaseUrl, initialLang, Array("@context"))
          .map({
            case (parsedContextNode, _, _) =>
              val contextObjectNode = parsedContextNode.get(1)
              (
                contextObjectNode.get("@base").asText,
                contextObjectNode.get("@language").asText
              )
          })
      )
      .getOrElse(Right((initialBaseUrl, initialLang)))
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
