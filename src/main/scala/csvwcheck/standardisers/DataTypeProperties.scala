package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.XsdDataTypes
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{DateFormatError, MetadataError}
import csvwcheck.models.DateFormat
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.standardisers.Utils.{MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning, parseUrlLinkProperty}
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import org.joda.time.DateTime
import shapeless.syntax.std.tuple.productTupleOps

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

object DataTypeProperties {
  def parseDataTypeProperty(
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

  private def validateRegEx(regexCandidate: String): Either[Exception, Unit] =
    try {
      regexCandidate.r
      Right(())
    } catch {
      case e: Exception =>
        Left(e)
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
}
