package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, BooleanNode, NullNode, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, ObjectPropertyParseResult, invalidValueWarning, parseJsonProperty}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object DialectProperties {
  private val validTrimValues = Array("true", "false", "start", "end")

  val parsers: Map[String, JsonNodeParser] = Map(
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "Dialect"),
    // Dialect Properties
    "commentPrefix" -> Utils.parseStringProperty(PropertyType.Dialect),
    "delimiter" -> Utils.parseStringProperty(PropertyType.Dialect),
    "doubleQuote" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "encoding" -> parseEncodingProperty(PropertyType.Dialect),
    "header" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "headerRowCount" -> Utils.parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "lineTerminators" -> parseLineTerminatorsProperty(PropertyType.Dialect),
    "quoteChar" -> Utils.parseStringProperty(PropertyType.Dialect),
    "skipBlankRows" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "skipColumns" -> Utils.parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "skipInitialSpace" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "skipRows" -> Utils.parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "trim" -> parseTrimProperty(PropertyType.Dialect),
  ) ++ IdProperty.parser

  def parseDialectProperty(
                            csvwPropertyType: PropertyType.Value
                          ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case objectNode: ObjectNode =>
        objectNode.fields.asScala
          .map(fieldAndValue =>
            parseDialectObjectProperty(
              baseUrl,
              lang,
              fieldAndValue.getKey,
              fieldAndValue.getValue
            )
          )
          .toObjectNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        // May be we might need to support dialect property of type other than ObjectNode.
        //  The dialect of a table is an object property. It could be provided as a URL that indicates
        //  a commonly used dialect, like this:
        //  "dialect": "http://example.org/tab-separated-values"
        Right(
          (
            NullNode.instance,
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseTrimProperty(
                         csvwPropertyType: PropertyType.Value
                       ): JsonNodeParser = { (value, _, _) =>
    value match {
      case boolNode: BooleanNode =>
        if (boolNode.booleanValue) {
          Right((new TextNode("true"), Array.empty, csvwPropertyType))
        } else {
          Right((new TextNode("false"), Array.empty, csvwPropertyType))
        }
      case textNode: TextNode if validTrimValues.contains(textNode.asText) =>
        Right((value, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("false"),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }

  private def parseLineTerminatorsProperty(
                          csvwPropertyType: PropertyType.Value
                        ): JsonNodeParser = { (value, _, _) =>
    value match {
//      case n: TextNode  => Right(Array(n.asText()))
//      case n: ArrayNode => Right(n.iterator().asScalaArray.map(_.asText()))
//      case n =>
//        Left(MetadataError(s"Unexpected node type ${n.getClass.getName}"))

      case a: ArrayNode => Right((a, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            BooleanNode.getFalse,
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }

  private def parseEncodingProperty(
                             csvwPropertyType: PropertyType.Value
                           ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode
        if Constants.ValidEncodings.contains(s.asText()) =>
        Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.instance,
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseDialectObjectProperty(
                                          baseUrl: String,
                                          lang: String,
                                          propertyName: String,
                                          valueNode: JsonNode
                                ): ObjectPropertyParseResult = {
    parseJsonProperty(parsers, propertyName, valueNode, baseUrl, lang)
      .map({
        case (parsedValueNode, propertyWarnings, propertyType) =>
          if (
            propertyType == PropertyType.Dialect && propertyWarnings.isEmpty
          ) {
            (propertyName, Some(parsedValueNode), propertyWarnings)
          } else {
            val warnings =
              if (propertyType != PropertyType.Dialect && propertyType != PropertyType.Common)
                propertyWarnings :+ "invalid_property"
              else
                propertyWarnings

            (propertyName, None, warnings)
          }
      })
  }
}
