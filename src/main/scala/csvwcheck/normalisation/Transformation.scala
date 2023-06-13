package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, NormalisationContext, Normaliser, invalidValueWarning, noWarnings, normaliseJsonProperty}
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Transformation {
  val normalisers: Map[String, Normaliser] = Map(
    // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#h-transformation-definitions
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Template"),
    "scriptFormat" -> Utils.normaliseUrlLinkProperty(PropertyType.Transformation),
    "source" -> Utils.normaliseStringProperty(PropertyType.Transformation),
    "targetFormat" -> Utils.normaliseUrlLinkProperty(PropertyType.Transformation),
    "titles" -> Utils.normaliseNaturalLanguageProperty(PropertyType.Transformation),
    "url" -> Utils.normaliseUrlLinkProperty(PropertyType.Transformation),
  ) ++ IdProperty.normaliser

  def normaliseTransformationsProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): Normaliser = { context => {
    context.node match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .zipWithIndex
          .map({
            case (transformationNode: ObjectNode, index) =>
              normaliseTransformationElement(context.toChild(transformationNode, index.toString))
            case (transformationNode, index) =>
              val elementContext = context.toChild(transformationNode, index.toString)
              Right(
                (None, Array(elementContext.makeWarning(s"invalid_transformation: ${transformationNode.toPrettyString}")))
              )
          })
          .toArrayNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode(0),
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def normaliseTransformationElement(context: NormalisationContext[ObjectNode]): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    context.node
      .getKeysAndValues
      .map({ case (propertyName, valueNode) =>
        val propertyContext = context.toChild(valueNode, propertyName)
        propertyName match {
          case _ =>
            normaliseJsonProperty(normalisers, propertyName, propertyContext)
              .map({
                case (
                  parsedTransformation,
                  Array(),
                  PropertyType.Transformation
                  ) =>
                  (propertyName, Some(parsedTransformation), noWarnings)
                case (_, warnings, PropertyType.Transformation) =>
                  (propertyName, None, warnings)
                case (_, warnings, propertyType) =>
                  (
                    propertyName,
                    None,
                    warnings :+ propertyContext.makeWarning(s"invalid_property '$propertyName' with type $propertyType")
                  )
              })
        }
      })
      .iterator
      .toObjectNodeAndWarnings
      .map({
        case (objectNode, warnings) => (Some(objectNode), warnings)
      })
  }
}
