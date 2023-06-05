package csvwcheck.propertyparser

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{DateFormatError, MetadataError}
import Utils.{
  JsonNodeParser,
  MetadataErrorsOrParsedArrayElements,
  MetadataErrorsOrParsedObjectProperties,ObjectPropertyParseResult, StringWarnings, invalidValueWarning}
import csvwcheck.models.DateFormat
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.propertyparser.Constants.{CsvWDataTypes, undefinedLanguage}
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import csvwcheck.{NameSpaces, XsdDataTypes}
import org.joda.time.DateTime
import shapeless.syntax.std.tuple.productTupleOps

import java.net.{URI, URL}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsScala}

object PropertyParser {

  private val PropertyParsers: Map[String, JsonNodeParser] = Map(
    // Context Properties
    "@language" -> parseLanguageProperty(PropertyType.Context),
    "@base" -> parseUrlLinkProperty(PropertyType.Context),
    // common properties
    "@id" -> parseUrlLinkProperty(PropertyType.Common),
    "dialect" -> parseDialectProperty(PropertyType.Common),
    "notes" -> parseNotesProperty(PropertyType.Common),
    "suppressOutput" -> Utils.parseBooleanProperty(PropertyType.Common),
    // Inherited properties
    "aboutUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "datatype" -> parseDataTypeProperty(PropertyType.Inherited),
    "default" -> Utils.parseStringProperty(PropertyType.Inherited),
    "lang" -> parseLanguageProperty(PropertyType.Inherited),
    "null" -> parseNullProperty(PropertyType.Inherited),
    "ordered" -> Utils.parseBooleanProperty(PropertyType.Inherited),
    "propertyUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "required" -> Utils.parseBooleanProperty(PropertyType.Inherited),
    "separator" -> parseSeparatorProperty(PropertyType.Inherited),
    "textDirection" -> parseTextDirectionProperty(PropertyType.Inherited),
    "valueUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    // Table properties
    "tableSchema" -> parseTableSchemaProperty(PropertyType.Table),
    "transformations" -> parseTransformationsProperty(PropertyType.Table),
    "url" -> parseUrlLinkProperty(PropertyType.Table),
    // Schema Properties
    "columns" -> ColumnProperties.parseColumnsProperty(PropertyType.Schema),
    "foreignKeys" -> parseForeignKeysProperty(PropertyType.Schema),
    "primaryKey" -> ForeignKeyProperties.parseColumnReferenceProperty(PropertyType.Schema),
    "rowTitles" -> ForeignKeyProperties.parseColumnReferenceProperty(PropertyType.Schema),
    // Column level properties
    "name" -> ColumnProperties.parseNameProperty(PropertyType.Column),
    "titles" -> ColumnProperties.parseNaturalLanguageProperty(PropertyType.Column),
    "virtual" -> Utils.parseBooleanProperty(PropertyType.Column),
    // Dialect Properties
    "commentPrefix" -> Utils.parseStringProperty(PropertyType.Dialect),
    "delimiter" -> Utils.parseStringProperty(PropertyType.Dialect),
    "doubleQuote" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "encoding" -> parseEncodingProperty(PropertyType.Dialect),
    "header" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "headerRowCount" -> parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "lineTerminators" -> parseArrayProperty(PropertyType.Dialect),
    "quoteChar" -> Utils.parseStringProperty(PropertyType.Dialect),
    "skipBlankRows" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "skipColumns" -> parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "skipInitialSpace" -> Utils.parseBooleanProperty(PropertyType.Dialect),
    "skipRows" -> parseNonNegativeIntegerProperty(PropertyType.Dialect),
    "trim" -> parseTrimProperty(PropertyType.Dialect),
    // Transformation properties
    "scriptFormat" -> parseScriptFormatProperty(PropertyType.Transformation),
    "source" -> parseSourceProperty(PropertyType.Transformation),
    "targetFormat" -> parseTargetFormatProperty(PropertyType.Transformation),
  ) ++ ForeignKeyProperties.parsers

  private val validTrimValues = Array("true", "false", "start", "end")

  def parseJsonProperty(
                         property: String,
                         value: JsonNode,
                         baseUrl: String,
                         lang: String
                       ): ParseResult[(JsonNode, StringWarnings, PropertyType.Value)] = {
    if (PropertyParsers.contains(property)) {
      PropertyParsers(property)(value, baseUrl, lang)
    } else if (
      RegExpressions.prefixedPropertyPattern
        .matches(property) && NameSpaces.values.contains(property.split(":")(0))
    ) {
      parseCommonPropertyValue(value, baseUrl, lang)
        .map(_ :+ PropertyType.Annotation)
    } else {
      // property name must be an absolute URI
      asUri(property)
        .map(_ => {
          try {
            parseCommonPropertyValue(value, baseUrl, lang)
              .map(_ :+ PropertyType.Annotation)
          } catch {
            case e: Exception =>
              Right(
                (
                  value,
                  Array[String](s"invalid_property ${e.getMessage}"),
                  PropertyType.Undefined
                )
              )
          }
        })
        .getOrElse(
          // Not a valid URI, but it isn't as bad as a MetadataError
          Right(
            (
              value,
              Array[String]("invalid_property"),
              PropertyType.Undefined
            )
          )
        )
    }
  }

  private def parseNullProperty(
                                 csvwPropertyType: PropertyType.Value
                               ): JsonNodeParser = { (value, _, _) => {
    value match {
      case _: TextNode => Right((value, Array.empty, csvwPropertyType))
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .map({
            case element: TextNode => Right((Some(element), Array[String]()))
            case _ =>
              Right((None, Array(invalidValueWarning)))
          })
          .toArrayNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode().add(""),
            if (value.isNull) Array.empty
            else Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseSeparatorProperty(
                                      csvwPropertyType: PropertyType.Value
                                    ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s if s.isTextual || s.isNull =>
        Right((s, Array.empty, csvwPropertyType))
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

  private def parseLanguageProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode
        if RegExpressions.Bcp47LanguagetagRegExp.matches(s.asText) =>
        Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode(""),
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseDataTypeObject(
                                   objectNode: ObjectNode,
                                   baseUrl: String,
                                   lang: String
                                 ): ParseResult[(ObjectNode, StringWarnings)] = {
    objectNode.fields.asScala
      .map(keyAndValue => {
        val key = keyAndValue.getKey
        val valueNode = keyAndValue.getValue

        key match {
          case "@id" =>
            parseDataTypeObjectIdNode(baseUrl, lang, valueNode).map("@id" +: _)
          case "base" =>
            val baseValue = valueNode.asText()
            if (XsdDataTypes.types.contains(baseValue)) {
              Right(
                (
                  "base",
                  Some(new TextNode(XsdDataTypes.types(baseValue))),
                  Array[String]()
                )
              )
            } else {
              Right(
                (
                  "base",
                  Some(new TextNode(XsdDataTypes.types("string"))),
                  Array("invalid_datatype_base")
                )
              )
            }
          case _ =>
            Right(
              (key, Some(valueNode), Array[String]())
            ) // todo: Is this right?
        }
      })
      .toObjectNodeAndStringWarnings
      .map({
        case (objectNode, stringWarnings) =>
          // Make sure that the `base` node is set.
          objectNode
            .getMaybeNode("base")
            .map(_ => (objectNode, stringWarnings))
            .getOrElse(
              (
                objectNode
                  .deepCopy()
                  .set("base", new TextNode(XsdDataTypes.types("string"))),
                stringWarnings
              )
            )
      })
  }

  private def parseDataTypeMinMaxValues(
                                         inputs: (ObjectNode, StringWarnings)
                                       ): ParseResult[(ObjectNode, StringWarnings)] = {
    val (dataTypeNode, stringWarnings) = inputs
    dataTypeNode
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          parseDataTypeMinMaxValuesForBaseType(
            dataTypeNode,
            stringWarnings,
            baseDataTypeNode.asText
          )
        case baseNode =>
          Left(
            MetadataError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(inputs))
  }

  private def parseDataTypeMinMaxValuesForBaseType(
                                                    dataTypeNode: ObjectNode,
                                                    existingStringWarnings: StringWarnings,
                                                    baseDataType: String
                                                  ): ParseResult[(ObjectNode, StringWarnings)] = {
    val minimumNode = dataTypeNode.getMaybeNode("minimum")
    val minInclusiveNode = dataTypeNode.getMaybeNode("minInclusive")
    val minExclusiveNode = dataTypeNode.getMaybeNode("minExclusive")
    val maximumNode = dataTypeNode.getMaybeNode("maximum")
    val maxInclusiveNode = dataTypeNode.getMaybeNode("maxInclusive")
    val maxExclusiveNode = dataTypeNode.getMaybeNode("maxExclusive")

    if (
      Constants.DateFormatDataTypes.contains(
        baseDataType
      ) || Constants.NumericFormatDataTypes.contains(
        baseDataType
      )
    ) {
      // Date and Numeric types are permitted min/max/etc. values
      parseMinMaxRanges(
        dataTypeNode,
        baseDataType,
        minimumNode,
        maximumNode,
        minInclusiveNode,
        minExclusiveNode,
        maxInclusiveNode,
        maxExclusiveNode,
        existingStringWarnings
      )
    } else {
      // Only date and numeric types as permitted min/max/etc. values

      val offendingNodes = Array(
        minimumNode,
        minInclusiveNode,
        minExclusiveNode,
        maximumNode,
        maxInclusiveNode,
        maxExclusiveNode
      ).filter(node => node.isDefined)
      if (offendingNodes.nonEmpty) {
        Left(
          MetadataError(
            "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types"
          )
        )
      } else {
        Right((dataTypeNode, existingStringWarnings))
      }
    }
  }

  private def validateRegEx(regexCandidate: String): Either[Exception, Unit] =
    try {
      regexCandidate.r
      Right(())
    } catch {
      case e: Exception =>
        Left(e)
    }

  private def parseDataTypeFormat(
                                   formatNode: JsonNode,
                                   baseDataType: String
                                 ): ParseResult[(Option[JsonNode], StringWarnings)] = {
    if (Constants.RegExpFormatDataTypes.contains(baseDataType)) {
      val regExFormat = formatNode.asText
      validateRegEx(regExFormat) match {
        case Right(()) => Right((Some(formatNode), Array.empty))
        case Left(e) =>
          Right(
            (None, Array(s"invalid_regex '$regExFormat' - ${e.getMessage}"))
          )
      }
    } else if (
      Constants.NumericFormatDataTypes.contains(baseDataType)
    ) {
      parseDataTypeFormatNumeric(formatNode)
        .map({
          case (parsedNode, stringWarnings) =>
            (Some(parsedNode), stringWarnings)
        })
    } else if (baseDataType == "http://www.w3.org/2001/XMLSchema#boolean") {
      formatNode match {
        case formatTextNode: TextNode =>
          val formatValues = formatNode.asText.split("""\|""")
          if (formatValues.length != 2) {
            Right((None, Array(s"invalid_boolean_format '$formatNode'")))
          } else {
            Right((Some(formatTextNode), Array.empty))
          }
        case _ =>
          // Boolean formats should always be textual
          Right((None, Array(s"invalid_boolean_format '$formatNode'")))
      }
    } else if (
      Constants.DateFormatDataTypes.contains(baseDataType)
    ) {
      formatNode match {
        case formatTextNode: TextNode =>
          val dateFormatString = formatTextNode.asText()
          try {
            val format = DateFormat(Some(dateFormatString), baseDataType).format
            if (format.isDefined) {
              Right((Some(new TextNode(format.get)), Array.empty))
            } else {
              Right((None, Array(s"invalid_date_format '$dateFormatString'")))
            }
          } catch {
            case _: DateFormatError =>
              Right((None, Array(s"invalid_date_format '$dateFormatString'")))
          }
        case _ =>
          Right((None, Array(s"invalid_date_format '$formatNode'")))
      }
    } else {
      Left(MetadataError(s"Unhandled format node ${formatNode.toPrettyString}"))
    }
  }

  private def parseDataTypeProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): (JsonNode, String, String) => ParseResult[
    (
      ObjectNode,
        StringWarnings,
        PropertyType.Value
      )
  ] = { (value, baseUrl, lang) => {
    initialDataTypePropertyParse(value, baseUrl, lang)
      .flatMap(parseDataTypeLengths)
      .flatMap(parseDataTypeMinMaxValues)
      .flatMap(parseDataTypeFormat)
      .map(_ :+ csvwPropertyType)
  }
  }

  private def parseDataTypeFormat(
                                   input: (ObjectNode, StringWarnings)
                                 ): ParseResult[(ObjectNode, StringWarnings)] =
    input match {
      case (dataTypeNode: ObjectNode, stringWarnings: StringWarnings) =>
        dataTypeNode
          .getMaybeNode("format")
          .map(formatNode => {
            for {
              baseDataType <- Utils.parseNodeAsText(dataTypeNode.get("base"))
              dataTypeFormatNodeAndWarnings <-
                parseDataTypeFormat(formatNode, baseDataType)
            } yield {
              val (formatNodeReplacement, newStringWarnings) =
                dataTypeFormatNodeAndWarnings

              val parsedDataTypeNode = formatNodeReplacement match {
                case Some(newNode) =>
                  dataTypeNode.deepCopy().set("format", newNode)
                case None =>
                  val modifiedDataTypeNode = dataTypeNode
                    .deepCopy()
                  modifiedDataTypeNode.remove("format")

                  modifiedDataTypeNode
              }
              (parsedDataTypeNode, stringWarnings ++ newStringWarnings)
            }
          })
          .getOrElse(Right((dataTypeNode, stringWarnings)))
    }

  private def parseUrlLinkProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (v, baseUrl, _) => {
    v match {
      case urlNode: TextNode =>
        val urlValue = urlNode.asText()
        if (RegExpressions.startsWithUnderscore.matches(urlValue)) {
          Left(MetadataError(s"URL $urlValue starts with _:"))
        } else {
          val baseUrlCopy = baseUrl match {
            case "" => urlValue
            case _ => new URL(new URL(baseUrl), urlValue).toString
          }
          Right(
            (new TextNode(baseUrlCopy), Array[String](), csvwPropertyType)
          )
        }
      case _ =>
        Right(
          (
            new TextNode(""),
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  @tailrec
  private def parseDataTypeFormatNumeric(
                                          formatNode: JsonNode
                                        ): ParseResult[(ObjectNode, StringWarnings)] = {
    formatNode match {
      case _: TextNode =>
        val formatObjectNode = JsonNodeFactory.instance.objectNode()
        parseDataTypeFormatNumeric(
          formatObjectNode.set("pattern", formatNode.deepCopy())
        )
      case formatObjectNode: ObjectNode =>
        Right(parseNumericFormatObjectNode(formatObjectNode))
      case _ =>
        Left(
          MetadataError(
            s"Unhandled numeric data type format ${formatNode.toPrettyString}"
          )
        )
    }
  }

  private def parseNumericFormatObjectNode(
                                            formatObjectNode: ObjectNode
                                          ): (ObjectNode, StringWarnings) = {
    def parseMaybeStringAt(propertyName: String): ParseResult[Option[String]] =
      formatObjectNode
        .getMaybeNode(propertyName)
        .map(Utils.parseNodeAsText(_).map(Some(_)))
        .getOrElse(Right(None))

    val numberFormatParserResult: ParseResult[Option[NumberParser]] = for {
      groupChar <- parseMaybeStringAt("groupChar")
        .map(_.map(_.charAt(0)))
      decimalChar <- parseMaybeStringAt("decimalChar")
        .map(_.map(_.charAt(0)))
      numberFormatParser <- parseMaybeStringAt("pattern")
        .flatMap( // Either map
          _.map(pattern => // Option map
            LdmlNumberFormatParser(
              groupChar.getOrElse(','),
              decimalChar.getOrElse('.')
            ).getParserForFormat(pattern)
              .map(Some(_))
          ).getOrElse(Right(None))
        )
    } yield numberFormatParser

    numberFormatParserResult match {
      case Right(_) => (formatObjectNode, Array[String]())
      case Left(err) =>
        val formatNodeWithoutPattern = formatObjectNode.deepCopy()
        formatNodeWithoutPattern.remove("pattern")

        (
          formatNodeWithoutPattern,
          Array(s"invalid_number_format - ${err.message}")
        )
    }
  }

  private def parseTableSchemaProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): JsonNodeParser = {
    def tableSchemaPropertyInternal(
                                     value: JsonNode,
                                     inheritedBaseUrlStr: String,
                                     inheritedLanguage: String
                                   ): ParseResult[(JsonNode, StringWarnings, PropertyType.Value)] = {
      val inheritedBaseUrl = new URL(inheritedBaseUrlStr)
      value match {
        case textNode: TextNode =>
          val schemaUrl = new URL(inheritedBaseUrl, textNode.asText())
          // todo: Need to use injected HTTP-boi to download the schema here.
          val schemaNode =
            objectMapper.readTree(schemaUrl).asInstanceOf[ObjectNode]

          val newIdValue = schemaNode
            .getMaybeNode("@id")
            .map(idNode => new URL(schemaUrl, idNode.asText))
            .orElse(Some(schemaUrl))
            .map(_.toString)

          parseTableSchemaContextBaseUrlAndLang(
            schemaNode,
            schemaUrl,
            inheritedLanguage
          ).flatMap({
            case (newBaseUrl, newLang) =>
              schemaNode.fields.asScala
                .map(fieldAndValue =>
                  parseTableSchemaObjectProperty(
                    fieldAndValue.getKey,
                    fieldAndValue.getValue,
                    newBaseUrl,
                    newLang,
                    newIdValue
                  )
                )
                .toObjectNodeAndStringWarnings
                .map({
                  case (objectNode, stringWarnings) =>
                    (objectNode, stringWarnings, csvwPropertyType)
                })
          })
        case schemaNode: ObjectNode =>
          schemaNode.fields.asScala
            .map(fieldAndValue =>
              parseTableSchemaObjectProperty(
                fieldAndValue.getKey,
                fieldAndValue.getValue,
                inheritedBaseUrl,
                inheritedLanguage,
                None
              )
            )
            .toObjectNodeAndStringWarnings
            .map({
              case (objectNode, stringWarnings) =>
                (objectNode, stringWarnings, csvwPropertyType)
            })
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

    tableSchemaPropertyInternal
  }

  private def parseTableSchemaObjectProperty(
                                              propertyName: String,
                                              value: JsonNode,
                                              baseUrl: URL,
                                              language: String,
                                              newIdValue: Option[String]
                                            ): ObjectPropertyParseResult = {
    propertyName match {
      case "@context" =>
        Right(
          (propertyName, None, Array.empty)
        ) // Need to remove context from the parsed object.
      case "@id" =>
        if (RegExpressions.startsWithUnderscore.matches(value.asText())) {
          Left(MetadataError(s"@id ${value.asText} starts with _:"))
        } else {
          val idNode = newIdValue
            .map(new TextNode(_))
            .orElse(Some(value))
          Right((propertyName, idNode, Array.empty))
        }
      case "@type" =>
        val declaredType = value.asText
        if (declaredType == "Schema") {
          Right((propertyName, Some(value), Array.empty))
        } else {
          Left(
            MetadataError(
              s"@type of schema is not 'Schema' (${value.toPrettyString})"
            )
          )
        }
      case _ =>
        parseJsonProperty(propertyName, value, baseUrl.toString, language)
          .map({
            case (parsedValue, stringWarnings@Array(), propType)
              if propType == PropertyType.Schema || propType == PropertyType.Inherited =>
              (propertyName, Some(parsedValue), stringWarnings)
            case (_, stringWarnings, propType)
              if propType == PropertyType.Schema || propType == PropertyType.Inherited =>
              (propertyName, None, stringWarnings)
            case (_, stringWarnings, _) =>
              (propertyName, None, stringWarnings :+ "invalid_property")
          })
    }
  }

  private def parseTableSchemaContextBaseUrlAndLang(
                                                     schemaJsonNode: ObjectNode,
                                                     inheritedBaseUrl: URL,
                                                     inheritedLang: String
                                                   ): ParseResult[(URL, String)] = {
    schemaJsonNode
      .getMaybeNode("@context")
      .map({
        case contextArrayNode: ArrayNode if contextArrayNode.size == 2 =>
          val contextElements = Array.from(contextArrayNode.asScala)
          contextElements.apply(1) match {
            case contextObjectNode: ObjectNode =>
              val baseUrl = contextObjectNode
                .getMaybeNode("@base")
                .map(baseNode => new URL(inheritedBaseUrl, baseNode.asText))
                .getOrElse(inheritedBaseUrl)
              val lang = contextObjectNode
                .getMaybeNode("@language")
                .map(_.asText)
                .getOrElse(inheritedLang)
              Right((baseUrl, lang))
            case unexpectedContextNode =>
              Left(
                MetadataError(
                  s"Unexpected context object $unexpectedContextNode"
                )
              )
          }
        case _ => Right((inheritedBaseUrl, inheritedLang))
      })
      .getOrElse(Right((inheritedBaseUrl, inheritedLang)))
  }

  private def parseForeignKeysProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .map(parseForeignKeyValue(_, baseUrl, lang))
          .toArrayNodeAndStringWarnings
          .map({
            case (foreignKeysArray, stringWarnings) =>
              (foreignKeysArray, stringWarnings, csvwPropertyType)
          })
      case _ =>
        Right(
          value,
          Array(invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  private def parseForeignKeyValue(
                                    foreignKey: JsonNode,
                                    baseUrl: String,
                                    lang: String
                                  ): ParseResult[(Option[JsonNode], Array[String])] = {
    foreignKey match {
      case foreignKeyObjectNode: ObjectNode =>
        foreignKeyObjectNode.fields.asScala
          .map(f => {
            val propertyName = f.getKey
            if (RegExpressions.containsColon.matches(propertyName)) {
              Left(
                MetadataError(
                  "foreignKey includes a prefixed (common) property"
                )
              )
            } else {
              parseJsonProperty(propertyName, f.getValue, baseUrl, lang)
                .map({
                  case (parsedNode, Array(), PropertyType.ForeignKey) =>
                    (propertyName, Some(parsedNode), Array[String]())
                  case (_, warnings, _) =>
                    (
                      propertyName,
                      None,
                      warnings :+ invalidValueWarning
                    )
                })
            }
          })
          .toObjectNodeAndStringWarnings
          .map({
            case (parsedNode, warningStrings) =>
              (Some(parsedNode), warningStrings)
          })
      case _ => Right(None, Array("invalid_foreign_key"))
    }
  }

  private def parseUriTemplateProperty(
                                        csvwPropertyType: PropertyType.Value
                                      ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode => Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode(""),
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseTextDirectionProperty(
                                          csvwPropertyType: PropertyType.Value
                                        ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode
        if Array[String]("ltr", "rtl", "inherit").contains(s.asText()) =>
        Right((s, Array.empty, csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("inherit"),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseEncodingProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode
        if Constants.ValidEncodings.contains(s.asText()) =>
        Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.instance,
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseArrayProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): JsonNodeParser = { (value, _, _) =>
    value match {
      case a: ArrayNode => Right((a, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            BooleanNode.getFalse,
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }

  private def parseTrimProperty(
                                 csvwPropertyType: PropertyType.Value
                               ): JsonNodeParser = { (value, _, _) =>
    value match {
      case boolNode: BooleanNode =>
        if (boolNode.booleanValue) {
          Right((new TextNode("true"), Array.empty, csvwPropertyType))
        } else {
          Right((new TextNode("false"), Array.empty, csvwPropertyType))
        }
      case textNode: TextNode if validTrimValues.contains(textNode.asText) =>
        Right((value, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("false"),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }

  private def parseTargetFormatProperty(
                                         csvwPropertyType: PropertyType.Value
                                       ): JsonNodeParser = { (value, _, _) =>
    Right((value, Array[String](), csvwPropertyType))
  }

  private def parseScriptFormatProperty(
                                         csvwPropertyType: PropertyType.Value
                                       ): JsonNodeParser = { (value, _, _) =>
    Right((value, Array[String](), csvwPropertyType))
  }

  private def parseSourceProperty(
                                   csvwPropertyType: PropertyType.Value
                                 ): JsonNodeParser = { (value, _, _) =>
    Right((value, Array[String](), csvwPropertyType))
  }

  private def parseDialectProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case objectNode: ObjectNode =>
        objectNode.fields.asScala
          .map(fieldAndValue =>
            parseDialectObjectProperty(
              baseUrl,
              lang,
              fieldAndValue.getKey,
              fieldAndValue.getValue
            )
          )
          .toObjectNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        // May be we might need to support dialect property of type other than ObjectNode.
        //  The dialect of a table is an object property. It could be provided as a URL that indicates
        //  a commonly used dialect, like this:
        //  "dialect": "http://example.org/tab-separated-values"
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

  private def parseTransformationsProperty(
                                            csvwPropertyType: PropertyType.Value
                                          ): JsonNodeParser = { (value, baseUrl, lang) => {
    value match {
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .zipWithIndex
          .map({
            case (transformation, index) =>
              transformation match {
                case o: ObjectNode =>
                  parseTransformationElement(o, index, baseUrl, lang)
                case _ =>
                  Right(
                    (None, Array(s"invalid_transformation $transformation"))
                  )
              }
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

  private def parseTransformationElement(
                                          transformationElement: ObjectNode,
                                          index: Int,
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
          case "@id" =>
            if (
              RegExpressions.startsWithUnderscore.matches(valueNode.asText())
            ) {
              Left(MetadataError(s"transformations[$index].@id starts with _:"))
            } else {
              Right((propertyName, Some(valueNode), Array[String]()))
            }
          case "@type" =>
            if (valueNode.asText() == "Template") {
              Right((propertyName, Some(valueNode), Array[String]()))
            } else {
              Left(
                MetadataError(
                  s"transformations[$index].@type  @type of transformation is not 'Template'"
                )
              )
            }
          // Hmm, really not sure about this random exclusion here.
          case "url" | "titles" =>
            Right((propertyName, Some(valueNode), Array[String]()))
          case _ =>
            parseJsonProperty(propertyName, valueNode, baseUrl, lang)
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

  private def parseMinMaxRanges(
                                 dataTypeNode: ObjectNode,
                                 baseDataType: String,
                                 minimumNode: Option[JsonNode],
                                 maximumNode: Option[JsonNode],
                                 minInclusiveNode: Option[JsonNode],
                                 minExclusiveNode: Option[JsonNode],
                                 maxInclusiveNode: Option[JsonNode],
                                 maxExclusiveNode: Option[JsonNode],
                                 stringWarnings: StringWarnings
                               ): ParseResult[(ObjectNode, StringWarnings)] = {

    if (minimumNode.isDefined) {
      dataTypeNode.put("minInclusive", minimumNode.map(_.asText()).get)
      dataTypeNode.remove("minimum")
    }

    if (maximumNode.isDefined) {
      dataTypeNode.put("maxInclusive", maximumNode.map(_.asText()).get)
      dataTypeNode.remove("maximum")
    }

    val minInclusive: Option[String] = getDataTypeRangeConstraint(
      minInclusiveNode
    )
    val maxInclusive: Option[String] = getDataTypeRangeConstraint(
      maxInclusiveNode
    )
    val minExclusive: Option[String] = getDataTypeRangeConstraint(
      minExclusiveNode
    )
    val maxExclusive: Option[String] = getDataTypeRangeConstraint(
      maxExclusiveNode
    )

    (minInclusive, minExclusive, maxInclusive, maxExclusive) match {
      case (Some(minI), Some(minE), _, _) =>
        Left(
          MetadataError(
            s"datatype cannot specify both minimum/minInclusive ($minI) and minExclusive ($minE)"
          )
        )
      case (_, _, Some(maxI), Some(maxE)) =>
        Left(
          MetadataError(
            s"datatype cannot specify both maximum/maxInclusive ($maxI) and maxExclusive ($maxE)"
          )
        )
      case _ =>
        if (
          Constants.NumericFormatDataTypes.contains(baseDataType)
        ) {
          parseMinMaxNumericRanges(
            dataTypeNode,
            stringWarnings,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive
          )
        } else if (
          Constants.DateFormatDataTypes.contains(baseDataType)
        ) {
          parseMinMaxDateTimeRanges(
            dataTypeNode,
            stringWarnings,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive
          )
        } else {
          throw new IllegalArgumentException(
            s"Base data type was neither numeric note date/time - $baseDataType"
          )
        }
    }
  }

  private def parseMinMaxDateTimeRanges(
                                         dataTypeNode: ObjectNode,
                                         stringWarnings: StringWarnings,
                                         minInclusive: Option[String],
                                         maxInclusive: Option[String],
                                         minExclusive: Option[String],
                                         maxExclusive: Option[String]
                                       ) = {
    (
      minInclusive.map(DateTime.parse),
      minExclusive.map(DateTime.parse),
      maxInclusive.map(DateTime.parse),
      maxExclusive.map(DateTime.parse)
    ) match {
      case (Some(minI), _, Some(maxI), _) if minI.getMillis > maxI.getMillis =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI.getMillis >= maxE.getMillis =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE.getMillis > maxE.getMillis =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE.getMillis >= maxI.getMillis =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }

  private def parseMinMaxNumericRanges(
                                        dataTypeNode: ObjectNode,
                                        stringWarnings: StringWarnings,
                                        minInclusive: Option[String],
                                        maxInclusive: Option[String],
                                        minExclusive: Option[String],
                                        maxExclusive: Option[String]
                                      ) = {
    (
      minInclusive.map(BigDecimal(_)),
      minExclusive.map(BigDecimal(_)),
      maxInclusive.map(BigDecimal(_)),
      maxExclusive.map(BigDecimal(_))
    ) match {
      case (Some(minI), _, Some(maxI), _) if minI > maxI =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI >= maxE =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE > maxE =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE >= maxI =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }

  private def parseDataTypeLengths(
                                    inputs: (ObjectNode, StringWarnings)
                                  ): ParseResult[(ObjectNode, StringWarnings)] = {
    val (dataTypeNode: ObjectNode, stringWarnings) = inputs

    dataTypeNode
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          parseDataTypeWithBase(
            dataTypeNode,
            baseDataTypeNode.asText,
            stringWarnings
          )
        case baseNode =>
          Left(
            MetadataError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(inputs))
  }

  private def parseDataTypeWithBase(
                                     dataTypeNode: ObjectNode,
                                     baseDataType: String,
                                     existingStringWarnings: StringWarnings
                                   ): ParseResult[(ObjectNode, StringWarnings)] = {
    val lengthNode = dataTypeNode.getMaybeNode("length")
    val minLengthNode = dataTypeNode.getMaybeNode("minLength")
    val maxLengthNode = dataTypeNode.getMaybeNode("maxLength")

    if (
      Constants.StringDataTypes.contains(
        baseDataType
      ) || Constants.BinaryDataTypes.contains(baseDataType)
    ) {
      // String and Binary data types are permitted length/minLength/maxLength fields.

      if (
        lengthNode.exists(len =>
          minLengthNode.exists(minLen => len.asInt < minLen.asInt)
        )
      ) {
        Left(
          MetadataError(
            s"datatype length (${lengthNode.map(_.asInt).get}) cannot be less than minLength (${minLengthNode.map(_.asInt).get})"
          )
        )
      } else if (
        lengthNode.exists(len =>
          maxLengthNode.exists(maxLen => len.asInt > maxLen.asInt)
        )
      ) {
        Left(
          MetadataError(
            s"datatype length (${lengthNode.map(_.asInt)}) cannot be more than maxLength (${
              maxLengthNode
                .map(_.asInt)
            })"
          )
        )
      } else if (
        minLengthNode
          .exists(min => maxLengthNode.exists(max => min.asInt > max.asInt))
      ) {
        Left(
          MetadataError(
            s"datatype minLength (${minLengthNode.map(_.asInt)}) cannot be more than maxLength (${
              maxLengthNode
                .map(_.asInt)
            })"
          )
        )
      } else {
        Right((dataTypeNode, existingStringWarnings))
      }
    } else {
      // length, minLength and maxLength are only permitted on String and Binary data types.
      if (lengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a length facet"
          )
        )
      } else if (minLengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a minLength facet"
          )
        )
      } else if (maxLengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a maxLength facet"
          )
        )
      } else {
        Right((dataTypeNode, existingStringWarnings))
      }
    }
  }

  private def initialDataTypePropertyParse(
                                            value: JsonNode,
                                            baseUrl: String,
                                            lang: String
                                          ): ParseResult[(ObjectNode, StringWarnings)] = {
    value match {
      case dataTypeObjectNode: ObjectNode =>
        parseDataTypeObject(dataTypeObjectNode, baseUrl, lang)
      case x: TextNode if XsdDataTypes.types.contains(x.asText()) =>
        Right(
          (
            objectMapper.createObjectNode
              .put("@id", XsdDataTypes.types(x.asText())),
            Array[String]()
          )
        )
      case _: TextNode =>
        Right(
          (
            objectMapper.createObjectNode
              .put("@id", XsdDataTypes.types("string")),
            Array(invalidValueWarning)
          )
        )
      case _ =>
        throw new IllegalArgumentException(s"Unhandled data type $value")
    }
  }

  private def parseDataTypeObjectIdNode(
                                         baseUrl: String,
                                         lang: String,
                                         valueNode: JsonNode
                                       ): ParseResult[(Option[JsonNode], StringWarnings)] = {
    val idValue = valueNode.asText()
    if (XsdDataTypes.types.values.toList.contains(idValue)) {
      Left(
        MetadataError(
          s"datatype @id must not be the id of a built-in datatype ($idValue)"
        )
      )
    } else {
      parseUrlLinkProperty(PropertyType.Common)(
        valueNode,
        baseUrl,
        lang
      ).map({
        case (linkNode, warns@Array(), _) => (Some(linkNode), warns)
        case (_, warns, _) => (None, warns)
      })
    }
  }

  private def getDataTypeRangeConstraint(
                                          maybeConstraintNode: Option[JsonNode]
                                        ): Option[String] = {
    maybeConstraintNode.flatMap(constraintNode =>
      constraintNode match {
        case rangeConstraint: ObjectNode =>
          rangeConstraint
            .getMaybeNode("dateTime")
            .map(rangeConstraint => rangeConstraint.asText())
        case rangeConstraint: TextNode =>
          Some(rangeConstraint.asText())
        case rangeConstraint: IntNode =>
          Some(rangeConstraint.asText())
        case rangeConstraint: DecimalNode =>
          Some(rangeConstraint.asText())
        case rangeConstraint: LongNode =>
          Some(rangeConstraint.asText())
      }
    )

  }

  private def parseDialectObjectProperty(
                                          baseUrl: String,
                                          lang: String,
                                          key: String,
                                          valueNode: JsonNode
                                        ): ObjectPropertyParseResult = {
    key match {
      case "@id" =>
        if (RegExpressions.startsWithUnderscore.matches(valueNode.asText())) {
          Left(MetadataError("@id starts with _:"))
        } else {
          Right(key, Some(valueNode), Array())
        }
      case "@type" =>
        if (valueNode.asText() == "Dialect") {
          Right(key, Some(valueNode), Array())
        } else {
          Left(MetadataError("@type of dialect is not 'Dialect'"))
        }
      case _ =>
        parseJsonProperty(key, valueNode, baseUrl, lang)
          .map({
            case (parsedValueNode, propertyWarnings, propertyType) =>
              if (
                propertyType == PropertyType.Dialect && propertyWarnings.isEmpty
              ) {
                (key, Some(parsedValueNode), propertyWarnings)
              } else {
                val warnings =
                  if (propertyType != PropertyType.Dialect)
                    propertyWarnings :+ "invalid_property"
                  else
                    propertyWarnings

                (key, None, warnings)
              }
          })
    }
  }

  private def asUri(property: String): Option[URI] =
    Option(new URI(property))
      .filter(uri => uri.getScheme != null && uri.getScheme.nonEmpty)

  private def parseCommonPropertyValue(
                                        commonPropertyValueNode: JsonNode,
                                        baseUrl: String,
                                        defaultLang: String
                                      ): ParseResult[(JsonNode, StringWarnings)] = {
    commonPropertyValueNode match {
      case o: ObjectNode => parseCommonPropertyObject(o, baseUrl, defaultLang)
      case _: TextNode =>
        defaultLang match {
          case lang if lang == undefinedLanguage =>
            Right((commonPropertyValueNode, Array()))
          case _ =>
            val objectNodeToReturn = JsonNodeFactory.instance.objectNode()
            objectNodeToReturn.set("@value", commonPropertyValueNode)
            objectNodeToReturn.set("@language", new TextNode(defaultLang))
            Right((objectNodeToReturn, Array()))
        }
      case a: ArrayNode =>
        a.elements()
          .asScala
          .map(elementNode =>
            parseCommonPropertyValue(elementNode, baseUrl, defaultLang)
              .map({
                case (parsedElementNode, warnings) =>
                  (Some(parsedElementNode), warnings)
              })
          )
          .toArrayNodeAndStringWarnings
      case _ =>
        Left(
          MetadataError(
            s"Unexpected common property value ${commonPropertyValueNode.toPrettyString}"
          )
        )
    }
  }

  private def parseCommonPropertyObject(
                                         objectNode: ObjectNode,
                                         baseUrl: String,
                                         defaultLang: String
                                       ): ParseResult[(ObjectNode, StringWarnings)] = {
    objectNode
      .fields()
      .asScala
      .map(fieldAndValue => {
        val propertyName = fieldAndValue.getKey
        val propertyValueNode = fieldAndValue.getValue
        (propertyName match {
          case "@context" | "@list" | "@set" =>
            Left(
              MetadataError(
                s"$propertyName: common property has $propertyName property"
              )
            )
          case "@type" =>
            parseCommonPropertyObjectType(
              objectNode,
              propertyName,
              propertyValueNode
            )
          case "@id" =>
            parseCommonPropertyObjectId(baseUrl, propertyValueNode)
              .map(v => (v, Array[String]()))
          case "@value" =>
            processCommonPropertyObjectValue(objectNode)
              .map(v => (v, Array[String]()))
          case "@language" =>
            parseCommonPropertyObjectLanguage(objectNode, propertyValueNode)
              .map(v => (v, Array[String]()))
          case _ =>
            if (propertyName(0).equals('@')) {
              Left(
                MetadataError(
                  s"common property has property other than @id, @type, @value or @language beginning with @ ($propertyName)"
                )
              )
            } else {
              parseCommonPropertyValue(propertyValueNode, baseUrl, defaultLang)
            }
        }).map({
          case (valueNode, warnings) =>
            (propertyName, Some(valueNode), warnings)
        })
      })
      .toObjectNodeAndStringWarnings
  }

  private def parseCommonPropertyObjectId(
                                           baseUrl: String,
                                           v: JsonNode
                                         ): ParseResult[JsonNode] = {
    if (baseUrl.isBlank) {
      Right(v)
    } else {
      Utils.parseNodeAsText(v)
        .flatMap(idValue => {
          if (RegExpressions.startsWithUnderscore.matches(idValue)) {
            Left(
              MetadataError(
                s"@id must not start with '_:'  -  $idValue"
              )
            )
          } else {
            val absoluteIdUrl = new URL(new URL(baseUrl), idValue)
            Right(new TextNode(absoluteIdUrl.toString))
          }
        })
    }
  }

  private def parseNonNegativeIntegerProperty(
                                               csvwPropertyType: PropertyType.Value
                                             ): JsonNodeParser = { (value, _, _) =>
    value match {
      case value: IntNode if value.asInt() >= 0 =>
        Right(value, Array[String](), csvwPropertyType)
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

  private def parseNotesProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): JsonNodeParser = {
    def parseNotesPropertyInternal(
                                    value: JsonNode,
                                    baseUrl: String,
                                    lang: String
                                  ): ParseResult[(JsonNode, Array[String], PropertyType.Value)] = {
      value match {
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .map(element =>
              parseCommonPropertyValue(element, baseUrl, lang)
                .map({
                  case (elementNode, warnings) => (Some(elementNode), warnings)
                })
            )
            .toArrayNodeAndStringWarnings
            .map({
              case (arrayNode, warnings) =>
                (arrayNode, warnings, csvwPropertyType)
            })
        case _ =>
          Right(
            (
              JsonNodeFactory.instance.arrayNode(),
              Array[String](invalidValueWarning),
              csvwPropertyType
            )
          )
      }
    }

    parseNotesPropertyInternal
  }

  private def processCommonPropertyObjectValue(
                                                value: ObjectNode
                                              ): ParseResult[JsonNode] = {
    if (
      (!value
        .path("@type")
        .isMissingNode) && (!value // todo: Stop using missing node
        .path("@language")
        .isMissingNode) // todo: Stop using missing node
    ) {
      Left(
        MetadataError(
          "common property with @value has both @language and @type"
        )
      )
    } else {
      var fieldNames = Array.from(value.fieldNames().asScala)
      fieldNames = fieldNames.filter(!_.contains("@type"))
      fieldNames = fieldNames.filter(!_.contains("@language"))
      if (fieldNames.length > 1) {
        Left(
          MetadataError(
            "common property with @value has properties other than @language or @type"
          )
        )
      } else {
        Right(value)
      }
    }
  }

  private def parseCommonPropertyObjectLanguage(
                                                 parentObjectNode: ObjectNode,
                                                 languageValueNode: JsonNode
                                               ): ParseResult[JsonNode] = {
    parentObjectNode
      .getMaybeNode("@value")
      .map(_ => {
        val language = languageValueNode.asText()
        if (language.isEmpty || !RegExpressions.Bcp47Language.r.matches(language)) {
          Left(
            MetadataError(
              s"common property has invalid @language ($language)"
            )
          )
        } else {
          Right(languageValueNode)
        }
      })
      .getOrElse(
        Left(MetadataError("common property with @language lacks a @value"))
      )
  }

  @tailrec
  private def parseCommonPropertyObjectType(
                                             objectNode: ObjectNode,
                                             p: String,
                                             v: JsonNode
                                           ): ParseResult[(JsonNode, StringWarnings)] = {
    val valueNode = objectNode.getMaybeNode("@value")
    v match {
      case s: TextNode =>
        val dataType = s.asText()

        val isCsvWDataType =
          valueNode.isEmpty && CsvWDataTypes.contains(dataType)
        val isXsdDataType =
          valueNode.isDefined && XsdDataTypes.types.contains(dataType)
        if (isCsvWDataType || isXsdDataType) {
          Right(s, Array.empty)
        } else {
          val arr: ArrayNode = JsonNodeFactory.instance.arrayNode()
          arr.add(s)
          parseCommonPropertyObjectType(objectNode, p, arr)
        }
      case a: ArrayNode =>
        a.elements()
          .asScala
          .map(typeElement => {
            val dataType = typeElement.asText()
            if (
              RegExpressions.prefixedPropertyPattern.matches(dataType) && NameSpaces.values
                .contains(dataType.split(":")(0))
            ) {
              Right(Some(a), Array[String]())
            } else {
              // typeElement Must be an absolute URI
              try {
                asUri(dataType)
                  .map(_ => Right(Some(a), Array[String]()))
                  .getOrElse(
                    Left(
                      MetadataError(
                        s"common property has invalid @type ($dataType)"
                      )
                    )
                  )
              } catch {
                case _: Exception =>
                  Left(
                    MetadataError(
                      s"common property has invalid @type ($dataType)"
                    )
                  )
              }
            }
          })
          .toArrayNodeAndStringWarnings
    }
  }


}
