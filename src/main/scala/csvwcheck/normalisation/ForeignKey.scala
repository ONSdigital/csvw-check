package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{Normaliser, MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, PropertyPath, invalidValueWarning, noWarnings, normaliseJsonProperty, parseNodeAsText}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object ForeignKey {
  val parsers: Map[String, Normaliser] = Map(
    // Foreign Key Properties
    "columnReference" -> Utils.normaliseColumnReferenceProperty(PropertyType.ForeignKey),
    "reference" -> normaliseForeignKeyReferenceProperty(PropertyType.ForeignKey),
    // foreignKey reference properties
    "resource" -> Utils.asAbsoluteUrl(PropertyType.ForeignKeyReference),
    "schemaReference" -> Utils.asAbsoluteUrl(PropertyType.ForeignKeyReference)
  )

  def normaliseForeignKeysProperty(
                                csvwPropertyType: PropertyType.Value
                              ): Normaliser = { (value, baseUrl, lang, propertyPath) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .zipWithIndex
          .map({ case (element, index) => normaliseForeignKeyValue(element, baseUrl, lang, propertyPath :+ index.toString) })
          .toArrayNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          JsonNodeFactory.instance.arrayNode(0),
          Array(MetadataWarning(propertyPath, invalidValueWarning)),
          csvwPropertyType
        )
    }
  }
  }

  private def normaliseForeignKeyReferenceProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): Normaliser = { (value, baseUrl, language, propertyPath) =>
    value match {
      case referenceObjectNode: ObjectNode =>
        normaliseForeignKeyReferenceObjectNode(
          referenceObjectNode,
          baseUrl,
          language,
          propertyPath
        ).map(_ :+ csvwPropertyType)
      case _ =>
        // If the supplied value of an object property is not a string or object (eg if it is an integer),
        // compliant applications MUST issue a warning and proceed as if the property had been specified as an
        // object with no properties.
        Right(
          (
            JsonNodeFactory.instance.objectNode(),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseForeignKeyReferenceObjectNode(
                                          foreignKeyObjectNode: ObjectNode,
                                          baseUrl: String,
                                          language: String,
                                          propertyPath: PropertyPath
                                        ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    val columnReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("columnReference")
    val resourceProperty = foreignKeyObjectNode.getMaybeNode("resource")
    val schemaReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("schemaReference")

    (columnReferenceProperty, resourceProperty, schemaReferenceProperty) match {
      case (None, _, _) =>
        Left(MetadataError("foreignKey reference columnReference is missing", propertyPath))
      case (_, None, None) =>
        Left(
          MetadataError(
            "foreignKey reference does not have either resource or schemaReference", propertyPath
          )
        )
      case (_, Some(_), Some(_)) =>
        Left(
          MetadataError(
            "foreignKey reference has both resource and schemaReference", propertyPath
          )
        )
      case _ =>
        foreignKeyObjectNode
          .fields()
          .asScala
          .map(keyValuePair => {
            val propertyName = keyValuePair.getKey
            val propertyValue = keyValuePair.getValue
            val localPropertyPath = propertyPath :+ propertyName
            // Check if property is included in the valid properties for a foreign key object
            if (parsers.contains(propertyName)) {
              normaliseJsonProperty(parsers, localPropertyPath, propertyName, propertyValue, baseUrl, language)
                .map({
                  case (newValue, Array(), _) =>
                    (propertyName, Some(newValue), noWarnings)
                  case (_, warnings, _) =>
                    // Throw away properties with warnings.
                    (propertyName, None, warnings)
                })
            } else if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                MetadataError(
                  s"foreignKey reference ($propertyName) includes a prefixed (common) property",
                  localPropertyPath
                )
              )
            } else {
              Right(
                (propertyName, None, Array(MetadataWarning(localPropertyPath, invalidValueWarning)))
              )
            }
          })
          .toObjectNodeAndWarnings
    }
  }

  private def normaliseForeignKeyValue(
                                    foreignKey: JsonNode,
                                    baseUrl: String,
                                    lang: String,
                                    propertyPath: PropertyPath
                                  ): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    foreignKey match {
      case foreignKeyObjectNode: ObjectNode =>
        foreignKeyObjectNode.fields.asScala
          .map(f => {
            val propertyName = f.getKey
            val localPropertyPath = propertyPath :+ propertyName
            if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                MetadataError(
                  "foreignKey includes a prefixed (common) property",
                  localPropertyPath
                )
              )
            } else {
              normaliseJsonProperty(parsers, localPropertyPath, propertyName, f.getValue, baseUrl, lang)
                .map({
                  case (parsedNode, Array(), PropertyType.ForeignKey) =>
                    (propertyName, Some(parsedNode), noWarnings)
                  case (_, warnings, _) =>
                    (
                      propertyName,
                      None,
                      warnings :+ MetadataWarning(localPropertyPath, invalidValueWarning)
                    )
                })
            }
          })
          .toObjectNodeAndWarnings
          .map({
            case (parsedNode, warnings) =>
              (Some(parsedNode), warnings)
          })
      case _ => Right(None, Array(MetadataWarning(propertyPath, "invalid_foreign_key")))
    }
  }
}
