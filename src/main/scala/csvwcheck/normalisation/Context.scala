package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{NormContext, Normaliser, noWarnings, parseNodeAsText}
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
    case normContext@NormContext(contextArrayNode: ArrayNode, _, _, _) =>
      contextArrayNode.elements().asScala.toArray match {
        case Array(csvwContextUrlNode: TextNode) => normaliseContext(propertyType)(normContext.withNode(csvwContextUrlNode))
        case Array(csvwContextUrlNode: TextNode, contextObjectNode: ObjectNode) if csvwContextUrlNode.asText() == csvwContextUri =>
          Utils.normaliseObjectNode(normalisers, normContext.toChild(contextObjectNode, "1"))
            .flatMap({
              case (objectNode, stringWarnings) =>
                for {
                  newBaseUrl <- objectNode.getMaybeNode("@base").map(parseNodeAsText(_)).getOrElse(Right(normContext.baseUrl))
                  newLang <- objectNode.getMaybeNode("@language").map(parseNodeAsText(_)).getOrElse(Right(normContext.language))
                } yield (getStandardContextNode(newBaseUrl, newLang), stringWarnings, propertyType)
            })
        case _ => Left(normContext.makeError(s"Unexpected @context value: ${contextArrayNode.toPrettyString}"))
      }
    case NormContext(contextNode: TextNode, baseUrl, lang, _) if contextNode.asText == csvwContextUri =>
      Right((getStandardContextNode(baseUrl, lang), noWarnings, propertyType))
    case NormContext(contextNode, _, _, propertyPath) => Left(MetadataError(s"Unexpected @context value: ${contextNode.toPrettyString}", propertyPath))
  }

  def getBaseUrlAndLanguageFromContext(rootNodeContext: NormContext[ObjectNode]): ParseResult[(String, String)] = {
    rootNodeContext.node
      .getMaybeNode("@context")
      .map(contextNode =>
        normaliseContext(PropertyType.Context)(rootNodeContext.toChild(contextNode,"@context"))
          .map({
            case (parsedContextNode, _, _) =>
              val contextObjectNode = parsedContextNode.get(1)
              (
                contextObjectNode.get("@base").asText,
                contextObjectNode.get("@language").asText
              )
          })
      )
      .getOrElse(Right((rootNodeContext.baseUrl, rootNodeContext.language)))
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
