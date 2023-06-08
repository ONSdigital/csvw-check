package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataWarning
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{Normaliser, MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, PropertyPath, invalidValueWarning, noWarnings, normaliseJsonProperty}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Transformation {
  val normalisers: Map[String, Normaliser] = Map(
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Template"),
    "scriptFormat" -> Utils.normaliseDoNothing(PropertyType.Transformation),
    "source" -> Utils.normaliseDoNothing(PropertyType.Transformation),
    "targetFormat" -> Utils.normaliseDoNothing(PropertyType.Transformation),
  ) ++ IdProperty.normaliser

  def normaliseTransformationsProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): Normaliser = { (value, baseUrl, lang, propertyPath) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .zipWithIndex
          .map({
                case (transformationNode: ObjectNode, index) =>
                  normaliseTransformationElement(transformationNode, baseUrl, lang, propertyPath :+ index.toString)
                case (transformationNode, index) =>
                  Right(
                    (None, Array(MetadataWarning(propertyPath :+ index.toString, s"invalid_transformation: ${transformationNode.toPrettyString}")))
                  )
          })
          .toArrayNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode(0),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  def normaliseTransformationElement(
                                  transformationElement: ObjectNode,
                                  baseUrl: String,
                                  lang: String,
                                  propertyPath: PropertyPath
                                ): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    transformationElement
      .fields()
      .asScala
      .map(keyValuePair => {
        val propertyName = keyValuePair.getKey
        val valueNode = keyValuePair.getValue
        val localPropertyPath = propertyPath :+ propertyName
        propertyName match {
          // todo: Hmm, really not sure about this random exclusion here.
          case "url" | "titles" =>
            Right((propertyName, Some(valueNode), noWarnings))
          case _ =>
            normaliseJsonProperty(normalisers, localPropertyPath, propertyName, valueNode, baseUrl, lang)
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
                    warnings :+ MetadataWarning(localPropertyPath, s"invalid_property '$propertyName' with type $propertyType")
                  )
              })
        }
      })
      .toObjectNodeAndWarnings
      .map({
        case (objectNode, warnings) => (Some(objectNode), warnings)
      })
  }
}
