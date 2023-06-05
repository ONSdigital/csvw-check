package csvwcheck.propertyparser

import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.propertyparser.RegExpressions.Bcp47LanguagetagRegExp
import shapeless.syntax.std.tuple.productTupleOps


import scala.jdk.CollectionConverters.IteratorHasAsScala

object ColumnProperties {
  private val NameRegExp =
    "^([A-Za-z0-9]|(%[A-F0-9][A-F0-9]))([A-Za-z0-9_]|(%[A-F0-9][A-F0-9]))*$".r

  def parseColumnsProperty(
                            csvwPropertyType: PropertyType.Value
                          ): JsonNodeParser = { (value, _, _) => {
    Right((value, Array[String](), csvwPropertyType))
  }
  }

  def parseNameProperty(
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

  def parseNaturalLanguageProperty(
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
