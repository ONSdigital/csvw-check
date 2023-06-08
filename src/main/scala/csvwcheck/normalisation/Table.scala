package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{Normaliser, MetadataErrorsOrParsedArrayElements, MetadataWarnings, PropertyPath, invalidValueWarning}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Table {
  private val normalisers: Map[String, Normaliser] = Map(
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Table"),
    // Table properties
    "dialect" -> Dialect.normaliseDialectProperty(PropertyType.Table),
    "notes" -> normaliseNotesProperty(PropertyType.Table),
    "suppressOutput" -> Utils.normaliseBooleanProperty(PropertyType.Table),
    "tableSchema" -> TableSchemaProperties.normaliseTableSchema(PropertyType.Table),
    "transformations" -> Transformation.normaliseTransformationsProperty(PropertyType.Table),
    "url" -> Utils.normaliseUrlLinkProperty(PropertyType.Table),
  ) ++ InheritedProperties.normalisers ++ IdProperty.normaliser

  def normaliseTable(propertyType: PropertyType.Value): Normaliser = (tableNode, baseUrl, lang, propertyPath) => tableNode match {
    case tableNode: ObjectNode =>
      Utils.normaliseObjectNode(tableNode, normalisers, baseUrl, lang, propertyPath)
        .map(_ :+ propertyType)
    case tableNode =>
      // Any items within an array that are not valid objects of the type expected are ignored
      Right(
        (
          NullNode.getInstance(),
          Array(MetadataWarning(propertyPath, s"Unexpected table value: ${tableNode.toPrettyString}")),
          propertyType
        )
      )
  }


  private def normaliseNotesProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): Normaliser = {
    def normaliseNotesPropertyInternal(
                                    value: JsonNode,
                                    baseUrl: String,
                                    lang: String,
                                    propertyPath: PropertyPath
                                  ): ParseResult[(JsonNode, MetadataWarnings, PropertyType.Value)] = {
      value match {
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .zipWithIndex
            .map({ case (element, index) =>
              Utils.normaliseCommonPropertyValue(element, baseUrl, lang, propertyPath :+ index.toString)
                .map({
                  case (elementNode, warnings) => (Some(elementNode), warnings)
                })
            })
            .toArrayNodeAndWarnings
            .map(_ :+ csvwPropertyType)
        case _ =>
          Right(
            (
              JsonNodeFactory.instance.arrayNode(),
              Array(MetadataWarning(propertyPath, invalidValueWarning)),
              csvwPropertyType
            )
          )
      }
    }

    normaliseNotesPropertyInternal
  }

}
