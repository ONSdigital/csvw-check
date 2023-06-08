package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import Utils.{MetadataErrorsOrParsedObjectProperties, MetadataWarnings, NormContext, Normaliser, PropertyPath, invalidValueWarning, noWarnings}
import com.fasterxml.jackson.databind.JsonNode
import csvwcheck.errors.{MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.RegExpressions.{Bcp47LanguagetagRegExp, NameRegExp}
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object Column {
  val normalisers: Map[String, Normaliser] = Map(
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Column"),
    // Column level properties
    "name" -> normaliseNameProperty(PropertyType.Column),
    "suppressOutput" -> Utils.normaliseBooleanProperty(PropertyType.Column),
    "titles" -> normaliseNaturalLanguageProperty(PropertyType.Column),
    "virtual" -> Utils.normaliseBooleanProperty(PropertyType.Column),
  ) ++ InheritedProperties.normalisers ++ IdProperty.normaliser


  def normaliseColumn(propertyType: PropertyType.Value): Normaliser = {
    case context => context.node match {
      case columnNode: ObjectNode =>
        Utils.normaliseObjectNode(normalisers, context.withNode(columnNode))
          .map(_ :+ propertyType)
      case columnNode =>
        // Any items within an array that are not valid objects of the type expected are ignored
        Right(
          (
            NullNode.getInstance(),
            Array(context.makeWarning(s"Unexpected column value: ${columnNode.toPrettyString}")),
            propertyType
          )
        )
    }
  }


  private def normaliseNameProperty(
                         csvwPropertyType: PropertyType.Value
                       ): Normaliser = { case context =>
    context.node match {
      case s: TextNode =>
        if (NameRegExp.matches(s.asText())) {
          Right((s, noWarnings, csvwPropertyType))
        } else {
          Right(
            (
              NullNode.instance,
              Array(context.makeWarning(invalidValueWarning)),
              csvwPropertyType
            )
          )
        }
      case _ =>
        Right(
          (
            NullNode.getInstance,
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def normaliseNaturalLanguageProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): Normaliser = { case context =>
    context.node match {
      case s: TextNode =>
        val languageMap = JsonNodeFactory.instance.objectNode()
        val arrayForLang = JsonNodeFactory.instance.arrayNode()
        arrayForLang.add(s.asText)
        languageMap.set(context.language, arrayForLang)
        Right((languageMap, noWarnings, csvwPropertyType))
      case a: ArrayNode =>
        val (validStrings, warnings) = getValidTextualElementsFromArray(context.withNode(a))
        val arrayNode: ArrayNode = objectMapper.valueToTree(validStrings)
        val languageMap = JsonNodeFactory.instance.objectNode()
        languageMap.set(context.language, arrayNode)
        Right((languageMap, warnings, csvwPropertyType))
      case languageMapObject: ObjectNode =>
        processNaturalLanguagePropertyObject(context.withNode(languageMapObject))
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            NullNode.getInstance(),
            Array(context.makeWarning(invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  private def processNaturalLanguagePropertyObject(context: NormContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] =
    context.node
      .getKeysAndValues
      .map({ case (propertyName, childValue) =>
        val childContext = context.toChild(childValue, propertyName)
        if (Bcp47LanguagetagRegExp.matches(propertyName)) {
          val (validStrings, warnings): (Array[String], MetadataWarnings) =
            childValue match {
              case s: TextNode => (Array(s.asText()), noWarnings)
              case arrayNode: ArrayNode => getValidTextualElementsFromArray(childContext.withNode(arrayNode))
              case _ =>
                (
                  Array[String](),
                  Array(
                    childContext.makeWarning(s"$invalidValueWarning - ${childValue.toPrettyString} is invalid, array or textual elements expected")
                  )
                )
            }
          val validStringsArrayNode: ArrayNode =
            objectMapper.valueToTree(validStrings)
          Right((propertyName, Some(validStringsArrayNode), warnings))
        } else {
          Right((propertyName, None, Array(childContext.makeWarning("invalid_language"))))
        }
      }).iterator.toObjectNodeAndWarnings


  private def getValidTextualElementsFromArray(context: NormContext[ArrayNode]): (Array[String], MetadataWarnings) =
    context.node.elements()
      .asScala
      .zipWithIndex
      .map({
        case (s: TextNode, _) => Right(s.asText())
        case (elementNode, index) =>
          val elementContext = context.toChild(elementNode, index.toString)
          Left(elementContext.makeWarning(context.node.toPrettyString + " is invalid, textual elements expected"))
      })
      .foldLeft((Array[String](), Array[MetadataWarning]()))({
        case ((validColumnNames, existingWarnings), Right(validColumnName)) =>
          (validColumnNames :+ validColumnName, existingWarnings)
        case ((validColumnNames, existingWarnings), Left(newWarning)) =>
          (validColumnNames, existingWarnings :+ newWarning)
      })


}
