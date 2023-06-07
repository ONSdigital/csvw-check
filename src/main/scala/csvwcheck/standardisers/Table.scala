package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedArrayElements, invalidValueWarning}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Table {
  private val parsers: Map[String, JsonNodeParser] = Map(
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "Table"),
    // Table properties
    "dialect" -> DialectProperties.parseDialectProperty(PropertyType.Table),
    "notes" -> parseNotesProperty(PropertyType.Table),
    "suppressOutput" -> Utils.parseBooleanProperty(PropertyType.Table),
    "tableSchema" -> TableSchemaProperties.parseTableSchema(PropertyType.Table),
    "transformations" -> TransformationProperties.parseTransformationsProperty(PropertyType.Table),
    "url" -> Utils.parseUrlLinkProperty(PropertyType.Table),
  ) ++ InheritedProperties.parsers ++ IdProperty.parser

  def parseTable(propertyType: PropertyType.Value): JsonNodeParser = (tableNode, baseUrl, lang) => tableNode match {
    case tableNode: ObjectNode =>
      Utils.parseObjectNode(tableNode, parsers, baseUrl, lang)
        .map(_ :+ propertyType)
    case tableNode => Left(MetadataError(s"Unexpected table value: ${tableNode.toPrettyString}"))
  }


  private def parseNotesProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): JsonNodeParser = {
    def parseNotesPropertyInternal(
                                    value: JsonNode,
                                    baseUrl: String,
                                    lang: String
                                  ): ParseResult[(JsonNode, Array[String], PropertyType.Value)] = {
      value match {
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .map(element =>
              Utils.parseCommonPropertyValue(element, baseUrl, lang)
                .map({
                  case (elementNode, warnings) => (Some(elementNode), warnings)
                })
            )
            .toArrayNodeAndStringWarnings
            .map(_ :+ csvwPropertyType)
        case _ =>
          Right(
            (
              JsonNodeFactory.instance.arrayNode(),
              Array[String](invalidValueWarning),
              csvwPropertyType
            )
          )
      }
    }

    parseNotesPropertyInternal
  }

}
