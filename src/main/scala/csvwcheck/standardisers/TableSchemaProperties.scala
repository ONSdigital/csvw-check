package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, NullNode, ObjectNode, TextNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, MetadataErrorsOrParsedArrayElements, ObjectPropertyParseResult, StringWarnings, invalidValueWarning}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import java.net.URL
import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsScala}

object TableSchemaProperties {
  private val parsers: Map[String, JsonNodeParser] = Map(
    "@context" -> Context.parseContext(PropertyType.Context),
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "Schema"),
    // Schema Properties
    "columns" -> parseColumnsProperty(PropertyType.Schema),
    "foreignKeys" -> ForeignKeyProperties.parseForeignKeysProperty(PropertyType.Schema),
    "primaryKey" -> Utils.parseColumnReferenceProperty(PropertyType.Schema),
    "rowTitles" -> Utils.parseColumnReferenceProperty(PropertyType.Schema),
  ) ++ InheritedProperties.parsers ++ IdProperty.parser

  def parseTableSchema(
                        csvwPropertyType: PropertyType.Value
                      ): JsonNodeParser = {
    def tableSchemaPropertyInternal(
                                     value: JsonNode,
                                     inheritedBaseUrlStr: String,
                                     inheritedLanguage: String
                                   ): ParseResult[(JsonNode, StringWarnings, PropertyType.Value)] = {
      val inheritedBaseUrl = new URL(inheritedBaseUrlStr)
      value match {
        case textNode: TextNode =>
          val schemaUrl = new URL(inheritedBaseUrl, textNode.asText())
          // todo: Need to use injected HTTP-boi to download the schema here.
          val schemaNode =
            objectMapper.readTree(schemaUrl).asInstanceOf[ObjectNode]

          parseSeparateTableSchemaBaseUrlAndLang(
            schemaNode,
            schemaUrl,
            inheritedLanguage
          ).flatMap({
            case (tableSchemaBaseUrl, newLang) =>
              schemaNode.fields.asScala
                .map(fieldAndValue =>
                  parseTableSchemaObjectProperty(
                    fieldAndValue.getKey,
                    fieldAndValue.getValue,
                    // N.B. The baseUrl of the child part of the document is now the URL where the table schema is
                    // defined.
                    tableSchemaBaseUrl,
                    newLang
                  )
                )
                .toObjectNodeAndStringWarnings
                .map(_ :+ csvwPropertyType)
          })
        case schemaNode: ObjectNode =>
          schemaNode.fields.asScala
            .map(fieldAndValue =>
              parseTableSchemaObjectProperty(
                fieldAndValue.getKey,
                fieldAndValue.getValue,
                inheritedBaseUrl,
                inheritedLanguage
              )
            )
            .toObjectNodeAndStringWarnings
            .map(_ :+ csvwPropertyType)
        case _ =>
          Right(
            (
              NullNode.getInstance(),
              Array(invalidValueWarning),
              csvwPropertyType
            )
          )
      }
    }

    tableSchemaPropertyInternal
  }



  private def parseTableSchemaObjectProperty(
                                              propertyName: String,
                                              value: JsonNode,
                                              tableSchemaBaseUrl: URL,
                                              language: String
                                            ): ObjectPropertyParseResult = {
      Utils.parseJsonProperty(parsers, propertyName, value, tableSchemaBaseUrl.toString, language)
        .map({
          case (parsedValue, stringWarnings@Array(), propType)
            if parsers.contains(propertyName) || propType == PropertyType.Common =>
            (propertyName, Some(parsedValue), stringWarnings)
          case (_, stringWarnings, propType)
            if parsers.contains(propertyName) || propType == PropertyType.Common =>
            (propertyName, None, stringWarnings)
          case (_, stringWarnings, _) =>
            (propertyName, None, stringWarnings :+ "invalid_property")
        })
  }

  private def parseSeparateTableSchemaBaseUrlAndLang(
                                                     schemaJsonNode: ObjectNode,
                                                     schemaBaseUrl: URL,
                                                     inheritedLang: String
                                                   ): ParseResult[(URL, String)] = {
    schemaJsonNode
      .getMaybeNode("@context")
      .map({
        case contextArrayNode: ArrayNode if contextArrayNode.size == 2 =>
          val contextElements = Array.from(contextArrayNode.asScala)
          contextElements.apply(1) match {
            case contextObjectNode: ObjectNode =>
              val baseUrl = contextObjectNode
                .getMaybeNode("@base")
                .map(baseNode => new URL(schemaBaseUrl, baseNode.asText))
                .getOrElse(schemaBaseUrl)
              val lang = contextObjectNode
                .getMaybeNode("@language")
                .map(_.asText)
                .getOrElse(inheritedLang)
              Right((baseUrl, lang))
            case unexpectedContextNode =>
              Left(
                MetadataError(
                  s"Unexpected context object $unexpectedContextNode"
                )
              )
          }
        case _ => Right((schemaBaseUrl, inheritedLang))
      })
      .getOrElse(Right((schemaBaseUrl, inheritedLang)))
  }

  private def parseColumnsProperty(
                            propertyType: PropertyType.Value
                          ): JsonNodeParser = (columnsNode, baseUrl, lang) => columnsNode match {
    case columnsNode: ArrayNode =>
      columnsNode.elements().asScala
        .map({
          case columnNode: ObjectNode => ColumnProperties.parseColumn(propertyType)(columnNode, baseUrl, lang)
            .map({ case (parsedColumnNode, stringWarnings, _) => (Some(parsedColumnNode), stringWarnings)})
          case columnNode => Left(MetadataError(s"Unexpected column: ${columnNode.toPrettyString}"))
        })
        .toArrayNodeAndStringWarnings
        .map(_ :+ propertyType)
    case columnsNode => Left(MetadataError(s"Unexpected columns value: ${columnsNode.toPrettyString}"))
  }
}
