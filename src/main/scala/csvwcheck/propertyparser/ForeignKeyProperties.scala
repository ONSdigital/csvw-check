package csvwcheck.propertyparser

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, NullNode, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.propertyparser.Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode

import java.net.URL
import scala.jdk.CollectionConverters.IteratorHasAsScala

object ForeignKeyProperties {
  val parsers: Map[String, JsonNodeParser] = Map(
    // Foreign Key Properties
    "columnReference" -> ForeignKeyProperties.parseColumnReferenceProperty(PropertyType.ForeignKey),
    "reference" -> ForeignKeyProperties.parseForeignKeyReferenceProperty(PropertyType.ForeignKey),
    // foreignKey reference properties
    "resource" -> ForeignKeyProperties.parseResourceProperty(PropertyType.ForeignKeyReference),
    "schemaReference" -> ForeignKeyProperties.parseSchemaReferenceProperty(
      PropertyType.ForeignKeyReference
    )
  )


  def parseResourceProperty(
                             csvwPropertyType: PropertyType.Value
                           ): JsonNodeParser = { (value, _, _) =>
    Right((value, Array[String](), csvwPropertyType))
  }

  def parseSchemaReferenceProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, baseUrl, _) =>
    value match {
      case textNode: TextNode =>
        val url = new URL(new URL(baseUrl), textNode.asText())
        Right((new TextNode(url.toString), Array[String](), csvwPropertyType))
      case _ =>
        Right((NullNode.instance, Array(invalidValueWarning), csvwPropertyType))
    }
  }

  def parseForeignKeyReferenceProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): JsonNodeParser = { (value, baseUrl, language) =>
    value match {
      case referenceObjectNode: ObjectNode =>
        parseForeignKeyReferenceObjectNode(
          referenceObjectNode,
          baseUrl,
          language
        ).map({
          case (parsedObject, stringWarnings) =>
            (parsedObject, stringWarnings, csvwPropertyType)
        })
      case _ => Left(MetadataError("foreignKey reference is not an object"))
    }
  }

  def parseColumnReferenceProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, _, _) => {
    value match {
      case textNode: TextNode =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode().add(textNode),
            Array.empty,
            csvwPropertyType
          )
        )
      case arrayNode: ArrayNode =>
        Right((arrayNode, Array.empty, csvwPropertyType))
      case _ =>
        Left(MetadataError(s"Unexpected column reference value $value"))
    }
  }
  }

  def parseForeignKeyReferenceObjectNode(
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
            if (
              Array[String]("resource", "schemaReference", "columnReference")
                .contains(propertyName)
            ) {
              parsers(propertyName)(propertyValue, baseUrl, language)
                .map({
                  case (newValue, Array(), _) =>
                    (propertyName, Some(newValue), Array[String]())
                  case (_, stringWarnings, _) =>
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
}
