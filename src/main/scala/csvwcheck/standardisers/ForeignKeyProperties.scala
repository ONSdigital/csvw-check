package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning, parseJsonProperty, parseNodeAsText}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object ForeignKeyProperties {
  val parsers: Map[String, JsonNodeParser] = Map(
    // Foreign Key Properties
    "columnReference" -> Utils.parseColumnReferenceProperty(PropertyType.ForeignKey),
    "reference" -> parseForeignKeyReferenceProperty(PropertyType.ForeignKey),
    // foreignKey reference properties
    "resource" -> Utils.asAbsoluteUrl(PropertyType.ForeignKeyReference),
    "schemaReference" -> Utils.asAbsoluteUrl(PropertyType.ForeignKeyReference)
  )

  def parseForeignKeysProperty(
                                csvwPropertyType: PropertyType.Value
                              ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .map(parseForeignKeyValue(_, baseUrl, lang))
          .toArrayNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          value,
          Array(invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  private def parseForeignKeyReferenceProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): JsonNodeParser = { (value, baseUrl, language) =>
    value match {
      case referenceObjectNode: ObjectNode =>
        parseForeignKeyReferenceObjectNode(
          referenceObjectNode,
          baseUrl,
          language
        ).map(_ :+ csvwPropertyType)
      case _ => Left(MetadataError("foreignKey reference is not an object"))
    }
  }

  private def parseForeignKeyReferenceObjectNode(
                                          foreignKeyObjectNode: ObjectNode,
                                          baseUrl: String,
                                          language: String
                                        ): ParseResult[(ObjectNode, StringWarnings)] = {
    val columnReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("columnReference")
    val resourceProperty = foreignKeyObjectNode.getMaybeNode("resource")
    val schemaReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("schemaReference")

    (columnReferenceProperty, resourceProperty, schemaReferenceProperty) match {
      case (None, _, _) =>
        Left(MetadataError("foreignKey reference columnReference is missing"))
      case (_, None, None) =>
        Left(
          MetadataError(
            "foreignKey reference does not have either resource or schemaReference"
          )
        )
      case (_, Some(_), Some(_)) =>
        Left(
          MetadataError(
            "foreignKey reference has both resource and schemaReference"
          )
        )
      case _ =>
        foreignKeyObjectNode
          .fields()
          .asScala
          .map(keyValuePair => {
            val propertyName = keyValuePair.getKey
            val propertyValue = keyValuePair.getValue
            // Check if property is included in the valid properties for a foreign key object
            if (parsers.contains(propertyName)) {
              parseJsonProperty(parsers, propertyName, propertyValue, baseUrl, language)
                .map({
                  case (newValue, Array(), _) =>
                    (propertyName, Some(newValue), Array[String]())
                  case (_, stringWarnings, _) =>
                    // Throw away properties with warnings.
                    (propertyName, None, stringWarnings)
                })
            } else if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                MetadataError(
                  s"foreignKey reference ($propertyName) includes a prefixed (common) property"
                )
              )
            } else {
              Right(
                (propertyName, None, Array(invalidValueWarning))
              )
            }
          })
          .toObjectNodeAndStringWarnings
    }
  }

  private def parseForeignKeyValue(
                                    foreignKey: JsonNode,
                                    baseUrl: String,
                                    lang: String
                                  ): ParseResult[(Option[JsonNode], Array[String])] = {
    foreignKey match {
      case foreignKeyObjectNode: ObjectNode =>
        foreignKeyObjectNode.fields.asScala
          .map(f => {
            val propertyName = f.getKey
            if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                MetadataError(
                  "foreignKey includes a prefixed (common) property"
                )
              )
            } else {
              parseJsonProperty(parsers, propertyName, f.getValue, baseUrl, lang)
                .map({
                  case (parsedNode, Array(), PropertyType.ForeignKey) =>
                    (propertyName, Some(parsedNode), Array[String]())
                  case (_, warnings, _) =>
                    (
                      propertyName,
                      None,
                      warnings :+ invalidValueWarning
                    )
                })
            }
          })
          .toObjectNodeAndStringWarnings
          .map({
            case (parsedNode, warningStrings) =>
              (Some(parsedNode), warningStrings)
          })
      case _ => Right(None, Array("invalid_foreign_key"))
    }
  }
}
