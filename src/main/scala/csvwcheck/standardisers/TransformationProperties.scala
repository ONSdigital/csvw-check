package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning, parseJsonProperty}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object TransformationProperties {
  val parsers: Map[String, JsonNodeParser] = Map(
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "Template"),
    "scriptFormat" -> Utils.parseDoNothing(PropertyType.Transformation),
    "source" -> Utils.parseDoNothing(PropertyType.Transformation),
    "targetFormat" -> Utils.parseDoNothing(PropertyType.Transformation),
  ) ++ IdProperty.parser

  def parseTransformationsProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .map({
                case transformationNode: ObjectNode =>
                  parseTransformationElement(transformationNode, baseUrl, lang)
                case transformationNode =>
                  Right(
                    (None, Array(s"invalid_transformation: ${transformationNode.toPrettyString}"))
                  )
          })
          .toArrayNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode(0),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  def parseTransformationElement(
                                  transformationElement: ObjectNode,
                                  baseUrl: String,
                                  lang: String
                                ): ParseResult[(Option[JsonNode], StringWarnings)] = {
    transformationElement
      .fields()
      .asScala
      .map(keyValuePair => {
        val propertyName = keyValuePair.getKey
        val valueNode = keyValuePair.getValue
        propertyName match {
          // todo: Hmm, really not sure about this random exclusion here.
          case "url" | "titles" =>
            Right((propertyName, Some(valueNode), Array[String]()))
          case _ =>
            parseJsonProperty(parsers, propertyName, valueNode, baseUrl, lang)
              .map({
                case (
                  parsedTransformation,
                  Array(),
                  PropertyType.Transformation
                  ) =>
                  (propertyName, Some(parsedTransformation), Array[String]())
                case (_, stringWarnings, PropertyType.Transformation) =>
                  (propertyName, None, stringWarnings)
                case (_, stringWarnings, propertyType) =>
                  (
                    propertyName,
                    None,
                    stringWarnings :+ s"invalid_property '$propertyName' with type $propertyType"
                  )
              })
        }
      })
      .toObjectNodeAndStringWarnings
      .map({
        case (objectNode, stringWarnings) => (Some(objectNode), stringWarnings)
      })
  }
}
