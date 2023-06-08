package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, BooleanNode, NullNode, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataWarning
import csvwcheck.normalisation.Utils.{Normaliser, MetadataErrorsOrParsedObjectProperties, ObjectPropertyNormaliserResult, PropertyPath, invalidValueWarning, noWarnings, normaliseJsonProperty}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Dialect {
  private val validTrimValues = Array("true", "false", "start", "end")

  val normalisers: Map[String, Normaliser] = Map(
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Dialect"),
    // Dialect Properties
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

  def normaliseDialectProperty(
                            csvwPropertyType: PropertyType.Value
                          ): Normaliser = { (value, baseUrl, lang, propertyPath) => {
    value match {
      case objectNode: ObjectNode =>
        objectNode.fields.asScala
          .map(fieldAndValue =>
            normaliseDialectObjectProperty(
              baseUrl,
              lang,
              fieldAndValue.getKey,
              fieldAndValue.getValue,
              propertyPath :+ fieldAndValue.getKey
            )
          )
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
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def normaliseTrimProperty(
                         csvwPropertyType: PropertyType.Value
                       ): Normaliser = { (value, _, _, propertyPath) =>
    value match {
      case boolNode: BooleanNode =>
        if (boolNode.booleanValue) {
          Right((new TextNode("true"), Array.empty, csvwPropertyType))
        } else {
          Right((new TextNode("false"), Array.empty, csvwPropertyType))
        }
      case textNode: TextNode if validTrimValues.contains(textNode.asText) =>
        Right((value, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("false"),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseLineTerminatorsProperty(
                          csvwPropertyType: PropertyType.Value
                        ): Normaliser = { (value, _, _, propertyPath) =>
    value match {
      // todo
//      case n: TextNode  => Right(Array(n.asText()))
//      case n: ArrayNode => Right(n.iterator().asScalaArray.map(_.asText()))
//      case n =>
//        Left(MetadataError(s"Unexpected node type ${n.getClass.getName}"))

      case a: ArrayNode => Right((a, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            BooleanNode.getFalse,
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseEncodingProperty(
                             csvwPropertyType: PropertyType.Value
                           ): Normaliser = { (value, _, _, propertyPath) => {
    value match {
      case s: TextNode
        if Constants.ValidEncodings.contains(s.asText()) =>
        Right((s, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.instance,
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def normaliseDialectObjectProperty(
                                          baseUrl: String,
                                          lang: String,
                                          propertyName: String,
                                          valueNode: JsonNode,
                                          propertyPath: PropertyPath
                                ): ObjectPropertyNormaliserResult = {
    normaliseJsonProperty(normalisers, propertyPath, propertyName, valueNode, baseUrl, lang)
      .map({
        case (parsedValueNode, propertyWarnings, propertyType) =>
          if (
            propertyType == PropertyType.Dialect && propertyWarnings.isEmpty
          ) {
            (propertyName, Some(parsedValueNode), propertyWarnings)
          } else {
            val warnings =
              if (propertyType != PropertyType.Dialect && propertyType != PropertyType.Common)
                propertyWarnings :+ MetadataWarning(propertyPath, "invalid_property")
              else
                propertyWarnings

            (propertyName, None, warnings)
          }
      })
  }
}
