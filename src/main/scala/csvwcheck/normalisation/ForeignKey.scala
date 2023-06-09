package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, NormContext, Normaliser, invalidValueWarning, noWarnings, normaliseJsonProperty}
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
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
                                  ): Normaliser = { context => {
    context.node match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .zipWithIndex
          .map({ case (element, index) => normaliseForeignKeyValue(context.toChild(element, index.toString)) })
          .toArrayNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          JsonNodeFactory.instance.arrayNode(0),
          Array(context.makeWarning(invalidValueWarning)),
          csvwPropertyType
        )
    }
  }
  }

  private def normaliseForeignKeyValue(context: NormContext[JsonNode]): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    context.node match {
      case foreignKeyObjectNode: ObjectNode =>
        foreignKeyObjectNode.getKeysAndValues
          .map({ case (propertyName, value) =>
            val propertyContext = context.toChild(value, propertyName)
            if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                propertyContext.makeError(
                  "foreignKey includes a prefixed (common) property"
                )
              )
            } else {
              normaliseJsonProperty(parsers, propertyName, propertyContext)
                .map({
                  case (parsedNode, Array(), PropertyType.ForeignKey) =>
                    (propertyName, Some(parsedNode), noWarnings)
                  case (_, warnings, _) =>
                    (
                      propertyName,
                      None,
                      warnings :+ propertyContext.makeWarning(invalidValueWarning)
                    )
                })
            }
          })
          .iterator
          .toObjectNodeAndWarnings
          .map({
            case (parsedNode, warnings) =>
              (Some(parsedNode), warnings)
          })
      case _ => Right(None, Array(context.makeWarning("invalid_foreign_key")))
    }
  }

  private def normaliseForeignKeyReferenceProperty(
                                                    csvwPropertyType: PropertyType.Value
                                                  ): Normaliser = { context =>
    context.node match {
      case referenceObjectNode: ObjectNode =>
        normaliseForeignKeyReferenceObjectNode(context.withNode(referenceObjectNode))
          .map(_ :+ csvwPropertyType)
      case _ =>
        // If the supplied value of an object property is not a string or object (eg if it is an integer),
        // compliant applications MUST issue a warning and proceed as if the property had been specified as an
        // object with no properties.
        Right(
          (
            JsonNodeFactory.instance.objectNode(),
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseForeignKeyReferenceObjectNode(context: NormContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] = {
    val foreignKeyObjectNode = context.node
    val columnReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("columnReference")
    val resourceProperty = foreignKeyObjectNode.getMaybeNode("resource")
    val schemaReferenceProperty =
      foreignKeyObjectNode.getMaybeNode("schemaReference")

    (columnReferenceProperty, resourceProperty, schemaReferenceProperty) match {
      case (None, _, _) =>
        Left(context.makeError("foreignKey reference columnReference is missing"))
      case (_, None, None) =>
        Left(
          context.makeError(
            "foreignKey reference does not have either resource or schemaReference"
          )
        )
      case (_, Some(_), Some(_)) =>
        Left(
          context.makeError(
            "foreignKey reference has both resource and schemaReference"
          )
        )
      case _ =>
        foreignKeyObjectNode
          .getKeysAndValues
          .map({ case (propertyName, value) =>
            val propertyContext = context.toChild(value, propertyName)
            // Check if property is included in the valid properties for a foreign key object
            if (parsers.contains(propertyName)) {
              normaliseJsonProperty(parsers, propertyName, propertyContext)
                .map({
                  case (newValue, Array(), _) =>
                    (propertyName, Some(newValue), noWarnings)
                  case (_, warnings, _) =>
                    // Throw away properties with warnings.
                    (propertyName, None, warnings)
                })
            } else if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                propertyContext.makeError(
                  s"foreignKey reference ($propertyName) includes a prefixed (common) property"
                )
              )
            } else {
              Right(
                (propertyName, None, Array(propertyContext.makeWarning(invalidValueWarning)))
              )
            }
          })
          .iterator
          .toObjectNodeAndWarnings
    }
  }
}
