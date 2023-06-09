package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, NormContext, Normaliser, ObjectPropertyNormaliserResult, invalidValueWarning, noWarnings, normaliseJsonProperty}
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Dialect {
  val normalisers: Map[String, Normaliser] = Map(
    // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#h-dialect-descriptions
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Dialect"),
    "commentPrefix" -> Utils.normaliseStringProperty(PropertyType.Dialect),
    "delimiter" -> Utils.normaliseStringProperty(PropertyType.Dialect),
    "doubleQuote" -> Utils.normaliseBooleanProperty(PropertyType.Dialect),
    "encoding" -> normaliseEncodingProperty(PropertyType.Dialect),
    "header" -> Utils.normaliseBooleanProperty(PropertyType.Dialect),
    "headerRowCount" -> Utils.normaliseNonNegativeIntegerProperty(PropertyType.Dialect),
    "lineTerminators" -> normaliseLineTerminatorsProperty(PropertyType.Dialect),
    "quoteChar" -> Utils.normaliseStringProperty(PropertyType.Dialect),
    "skipBlankRows" -> Utils.normaliseBooleanProperty(PropertyType.Dialect),
    "skipColumns" -> Utils.normaliseNonNegativeIntegerProperty(PropertyType.Dialect),
    "skipInitialSpace" -> Utils.normaliseBooleanProperty(PropertyType.Dialect),
    "skipRows" -> Utils.normaliseNonNegativeIntegerProperty(PropertyType.Dialect),
    "trim" -> normaliseTrimProperty(PropertyType.Dialect),
  ) ++ IdProperty.normaliser
  private val validTrimValues = Array("true", "false", "start", "end")

  def normaliseDialectProperty(
                                csvwPropertyType: PropertyType.Value
                              ): Normaliser = { context => {
    context.node match {
      case objectNode: ObjectNode =>
        objectNode.getKeysAndValues
          .map({ case (propertyName, value) => normaliseDialectObjectProperty(propertyName, context.toChild(value, propertyName)) })
          .iterator
          .toObjectNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        // May be we might need to support dialect property of type other than ObjectNode.
        //  The dialect of a table is an object property. It could be provided as a URL that indicates
        //  a commonly used dialect, like this:
        //  "dialect": "http://example.org/tab-separated-values"
        Right(
          (
            NullNode.instance,
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def normaliseDialectObjectProperty(propertyName: String, propertyContext: NormContext[JsonNode]): ObjectPropertyNormaliserResult = {
    normaliseJsonProperty(normalisers, propertyName, propertyContext)
      .map({
        case (parsedValueNode, propertyWarnings, propertyType) =>
          if (
            propertyType == PropertyType.Dialect && propertyWarnings.isEmpty
          ) {
            (propertyName, Some(parsedValueNode), propertyWarnings)
          } else {
            val warnings =
              if (propertyType != PropertyType.Dialect && propertyType != PropertyType.Common)
                propertyWarnings :+ propertyContext.makeWarning("invalid_property")
              else
                propertyWarnings

            (propertyName, None, warnings)
          }
      })
  }

  private def normaliseTrimProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): Normaliser = { context =>
    context.node match {
      case boolNode: BooleanNode =>
        if (boolNode.booleanValue) {
          Right((new TextNode("true"), Array.empty, csvwPropertyType))
        } else {
          Right((new TextNode("false"), Array.empty, csvwPropertyType))
        }
      case textNode: TextNode if validTrimValues.contains(textNode.asText) =>
        Right((textNode, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("false"),
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseLineTerminatorsProperty(
                                                csvwPropertyType: PropertyType.Value
                                              ): Normaliser = { context =>
    context.node match {
      case lineTerminator: TextNode if !lineTerminator.asText.isEmpty =>
        val lineTerminatorsArray = JsonNodeFactory.instance.arrayNode(1)
        lineTerminatorsArray.add(lineTerminator)
        Right(lineTerminatorsArray, noWarnings, csvwPropertyType)
      case lineTerminatorsArray: ArrayNode =>
        lineTerminatorsArray.elements()
          .asScala
          .map({
            case lineTerminatorElement: TextNode if !lineTerminatorElement.asText.isEmpty =>
              Right((Some(lineTerminatorElement), noWarnings))
            case lineTerminatorElement =>
              Right(
                (
                  None,
                  Array(context.makeWarning(s"Unexpected line terminator value: ${lineTerminatorElement.toPrettyString}"))
                )
              )
          })
          .toArrayNodeAndWarnings
        Right((lineTerminatorsArray, noWarnings, csvwPropertyType))
      case lineTerminatorValue =>
        // Any items within an array that are not valid objects of the type expected are ignored
        Right(
          (
            NullNode.getInstance(),
            Array(context.makeWarning(s"Unexpected line terminator value: ${lineTerminatorValue.toPrettyString}")),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseEncodingProperty(
                                         csvwPropertyType: PropertyType.Value
                                       ): Normaliser = { context => {
    context.node match {
      case s: TextNode
        if Constants.ValidEncodings.contains(s.asText()) =>
        Right((s, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.instance,
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }
}
