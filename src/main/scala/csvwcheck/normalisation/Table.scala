package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedArrayElements, MetadataWarnings, NormContext, Normaliser, PropertyPath, invalidValueWarning}
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

  def normaliseTable(propertyType: PropertyType.Value): Normaliser = context => context.node match {
    case tableNode: ObjectNode =>
      Utils.normaliseObjectNode(normalisers, context.withNode(tableNode))
        .map(_ :+ propertyType)
    case tableNode =>
      // Any items within an array that are not valid objects of the type expected are ignored
      Right(
        (
          NullNode.getInstance(),
          Array(context.makeWarning(s"Unexpected table value: ${tableNode.toPrettyString}")),
          propertyType
        )
      )
  }


  private def normaliseNotesProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): Normaliser = {
    def normaliseNotesPropertyInternal(context: NormContext[JsonNode]): ParseResult[(JsonNode, MetadataWarnings, PropertyType.Value)] = {
      context.node match {
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .zipWithIndex
            .map({ case (element, index) =>
              Utils.normaliseCommonPropertyValue(context.toChild(element, index.toString))
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
              Array(context.makeWarning(invalidValueWarning)),
              csvwPropertyType
            )
          )
      }
    }

    normaliseNotesPropertyInternal
  }

}
