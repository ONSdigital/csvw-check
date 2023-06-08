package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import Utils.{Normaliser, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, PropertyPath, invalidValueWarning, noWarnings}
import csvwcheck.errors.{MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.RegExpressions.{Bcp47LanguagetagRegExp, NameRegExp}
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


  def normaliseColumn(propertyType: PropertyType.Value): Normaliser = (columnNode, baseUrl, lang, propertyPath) => columnNode match {
    case columnNode: ObjectNode =>
      Utils.normaliseObjectNode(columnNode, normalisers, baseUrl, lang, propertyPath)
        .map(_ :+ propertyType)
    case columnNode =>
      // Any items within an array that are not valid objects of the type expected are ignored
      Right(
        (
          NullNode.getInstance(),
          Array(MetadataWarning(propertyPath, s"Unexpected column value: ${columnNode.toPrettyString}")),
          propertyType
        )
      )
  }


  private def normaliseNameProperty(
                         csvwPropertyType: PropertyType.Value
                       ): Normaliser = { (value, _, _, propertyPath) => {
    value match {
      case s: TextNode =>
        if (NameRegExp.matches(s.asText())) {
          Right((s, noWarnings, csvwPropertyType))
        } else {
          Right(
            (
              NullNode.instance,
              Array(MetadataWarning(propertyPath, invalidValueWarning)),
              csvwPropertyType
            )
          )
        }
      case _ =>
        Right(
          (
            NullNode.getInstance,
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def normaliseNaturalLanguageProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): Normaliser = { (value, _, lang, propertyPath) => {
    value match {
      case s: TextNode =>
        val languageMap = JsonNodeFactory.instance.objectNode()
        val arrayForLang = JsonNodeFactory.instance.arrayNode()
        arrayForLang.add(s.asText)
        languageMap.set(lang, arrayForLang)
        Right((languageMap, noWarnings, csvwPropertyType))
      case a: ArrayNode =>
        val (validStrings, warnings) = getValidTextualElementsFromArray(a, propertyPath)
        val arrayNode: ArrayNode = objectMapper.valueToTree(validStrings)
        val languageMap = JsonNodeFactory.instance.objectNode()
        languageMap.set(lang, arrayNode)
        Right((languageMap, warnings, csvwPropertyType))
      case languageMapObject: ObjectNode =>
        processNaturalLanguagePropertyObject(languageMapObject, propertyPath)
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            NullNode.getInstance(),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def processNaturalLanguagePropertyObject(
                                                    value: ObjectNode,
                                                    propertyPath: PropertyPath
                                                  ): ParseResult[(ObjectNode, MetadataWarnings)] =
    value.fields.asScala
      .map(fieldAndValue => {
        val propertyName = fieldAndValue.getKey
        val localPropertyPath = propertyPath :+ propertyName
        if (Bcp47LanguagetagRegExp.matches(propertyName)) {
          val (validStrings, warnings): (Array[String], MetadataWarnings) = {
            fieldAndValue.getValue match {
              case s: TextNode => (Array(s.asText()), noWarnings)
              case a: ArrayNode => getValidTextualElementsFromArray(a, localPropertyPath)
              case _ =>
                (
                  Array.empty,
                  Array(
                    MetadataWarning(localPropertyPath,
                      s"$invalidValueWarning - ${fieldAndValue.getValue.toPrettyString} is invalid, array or textual elements expected",
                    )
                  )
                )
            }
          }
          val validStringsArrayNode: ArrayNode =
            objectMapper.valueToTree(validStrings)
          Right((propertyName, Some(validStringsArrayNode), warnings))
        } else {
          Right((propertyName, None, Array(MetadataWarning(localPropertyPath, "invalid_language"))))
        }
      })
      .toObjectNodeAndWarnings

  private def getValidTextualElementsFromArray(
                                                a: ArrayNode,
                                                propertyPath: PropertyPath
                                              ): (Array[String], MetadataWarnings) =
    a.elements()
      .asScala
      .zipWithIndex
      .map({
        case (s: TextNode, _) => Right(s.asText())
        case (_, index) =>
          val elementPropertyPath = propertyPath :+ index.toString
          Left(MetadataWarning(elementPropertyPath, a.toPrettyString + " is invalid, textual elements expected"))
      })
      .foldLeft((Array[String](), Array[MetadataWarning]()))({
        case ((validColumnNames, existingWarnings), Right(validColumnName)) =>
          (validColumnNames :+ validColumnName, existingWarnings)
        case ((validColumnNames, existingWarnings), Left(newWarning)) =>
          (validColumnNames, existingWarnings :+ newWarning)
      })


}
