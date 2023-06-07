package csvwcheck.standardisers

import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning}
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.RegExpressions.Bcp47LanguagetagRegExp
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object ColumnProperties {
  val parsers: Map[String, JsonNodeParser] = Map(
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "Column"),
    // Column level properties
    "name" -> parseNameProperty(PropertyType.Column),
    "suppressOutput" -> Utils.parseBooleanProperty(PropertyType.Column),
    "titles" -> parseNaturalLanguageProperty(PropertyType.Column),
    "virtual" -> Utils.parseBooleanProperty(PropertyType.Column),
  ) ++ InheritedProperties.parsers ++ IdProperty.parser

  private val NameRegExp =
    "^([A-Za-z0-9]|(%[A-F0-9][A-F0-9]))([A-Za-z0-9_]|(%[A-F0-9][A-F0-9]))*$".r


  def parseColumn(propertyType: PropertyType.Value): JsonNodeParser = (columnNode, baseUrl, lang) => columnNode match {
    case columnNode: ObjectNode =>
      Utils.parseObjectNode(columnNode, parsers, baseUrl, lang)
        .map(_ :+ propertyType)
    case columnNode => Left(MetadataError(s"Unexpected table value: ${columnNode.toPrettyString}"))
  }


  private def parseNameProperty(
                         csvwPropertyType: PropertyType.Value
                       ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode =>
        if (NameRegExp.matches(s.asText())) {
          Right((s, Array.empty, csvwPropertyType))
        } else {
          Right(
            (
              NullNode.instance,
              Array(invalidValueWarning),
              csvwPropertyType
            )
          )
        }
      case _ =>
        Right(
          (
            NullNode.instance,
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseNaturalLanguageProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, _, lang) => {
    value match {
      case s: TextNode =>
        val languageMap = JsonNodeFactory.instance.objectNode()
        val arrayForLang = JsonNodeFactory.instance.arrayNode()
        arrayForLang.add(s.asText)
        languageMap.set(lang, arrayForLang)
        Right((languageMap, Array[String](), csvwPropertyType))
      case a: ArrayNode =>
        val (validStrings, warnings) = getValidTextualElementsFromArray(a)
        val arrayNode: ArrayNode = objectMapper.valueToTree(validStrings)
        val languageMap = JsonNodeFactory.instance.objectNode()
        languageMap.set(lang, arrayNode)
        Right((languageMap, warnings, csvwPropertyType))
      case languageMapObject: ObjectNode =>
        processNaturalLanguagePropertyObject(languageMapObject)
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            NullNode.getInstance(),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def processNaturalLanguagePropertyObject(
                                                    value: ObjectNode
                                                  ): ParseResult[(ObjectNode, StringWarnings)] =
    value.fields.asScala
      .map(fieldAndValue => {
        val elementKey = fieldAndValue.getKey
        if (Bcp47LanguagetagRegExp.matches(elementKey)) {
          val (validStrings, warnings): (Array[String], StringWarnings) =
            fieldAndValue.getValue match {
              case s: TextNode => (Array(s.asText()), Array[String]())
              case a: ArrayNode => getValidTextualElementsFromArray(a)
              case _ =>
                (
                  Array.empty,
                  Array(
                    fieldAndValue.getValue.toPrettyString + " is invalid, array or textual elements expected",
                    invalidValueWarning
                  )
                )
            }
          val validStringsArrayNode: ArrayNode =
            objectMapper.valueToTree(validStrings)
          Right((elementKey, Some(validStringsArrayNode), warnings))
        } else {
          Right((elementKey, None, Array("invalid_language")))
        }
      })
      .toObjectNodeAndStringWarnings

  private def getValidTextualElementsFromArray(
                                                a: ArrayNode
                                              ): (Array[String], StringWarnings) =
    a.elements()
      .asScala
      .map({
        case s: TextNode => Right(s.asText())
        case _ =>
          Left(a.toPrettyString + " is invalid, textual elements expected")
      })
      .foldLeft((Array[String](), Array[String]()))({
        case ((validColumnNames, stringWarnings), Right(validColumnName)) =>
          (validColumnNames :+ validColumnName, stringWarnings)
        case ((validColumnNames, stringWarnings), Left(stringWarning)) =>
          (validColumnNames, stringWarnings :+ stringWarning)
      })


}
