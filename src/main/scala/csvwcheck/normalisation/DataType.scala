package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.XsdDataTypes
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{DateFormatError, MetadataError, MetadataWarning}
import csvwcheck.models.DateFormat
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedObjectProperties, MetadataWarnings, PropertyPath, invalidValueWarning, noWarnings, parseNodeAsText, normaliseUrlLinkProperty}
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import org.joda.time.DateTime
import shapeless.syntax.std.tuple.productTupleOps

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

object DataType {
  def normaliseDataTypeProperty(
                             csvwPropertyType: PropertyType.Value
                           ): (JsonNode, String, String, PropertyPath) => ParseResult[
    (
      ObjectNode,
        MetadataWarnings,
        PropertyType.Value
      )
  ] = { (value, baseUrl, lang, propertyPath) => {
    initialDataTypePropertyNormalisation(value, baseUrl, lang, propertyPath)
      .flatMap(normaliseDataTypeLengths(_, propertyPath))
      .flatMap(normaliseDataTypeMinMaxValues(_, propertyPath))
      .flatMap(normaliseDataTypeFormat(_, propertyPath))
      .map(_ :+ csvwPropertyType)
  }
  }

  private def normaliseDataTypeFormat(
                                   input: (ObjectNode, MetadataWarnings),
                                   propertyPath: PropertyPath
                                 ): ParseResult[(ObjectNode, MetadataWarnings)] =
    input match {
      case (dataTypeNode: ObjectNode, warnings: MetadataWarnings) =>
        dataTypeNode
          .getMaybeNode("format")
          .map(formatNode => {
            val formatNodePropertyPath = propertyPath :+ "format"
            for {
              baseDataType <- Utils.parseNodeAsText(dataTypeNode.get("base"))
              dataTypeFormatNodeAndWarnings <-
                normaliseDataTypeFormat(formatNode, baseDataType, formatNodePropertyPath)
            } yield {
              val (formatNodeReplacement, newWarnings) =
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
              (parsedDataTypeNode, warnings ++ newWarnings)
            }
          })
          .getOrElse(Right((dataTypeNode, warnings)))
    }

  private def normaliseDataTypeFormat(
                                   formatNode: JsonNode,
                                   baseDataType: String,
                                   propertyPath: PropertyPath
                                 ): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    if (Constants.RegExpFormatDataTypes.contains(baseDataType)) {
      parseNodeAsText(formatNode)
        .flatMap(regExFormat =>
          validateRegEx(regExFormat) match {
            case Right(()) => Right((Some(new TextNode(regExFormat)), noWarnings))
            case Left(e) =>
              Right(
                (None, Array(MetadataWarning(propertyPath, s"invalid_regex '$regExFormat' - ${e.getMessage}")))
              )
          }
        )
    } else if (
      Constants.NumericFormatDataTypes.contains(baseDataType)
    ) {
      normaliseDataTypeFormatNumeric(formatNode, propertyPath)
        .map({
          case (parsedNode, warnings) =>
            (Some(parsedNode), warnings)
        })
    } else if (baseDataType == "http://www.w3.org/2001/XMLSchema#boolean") {
      formatNode match {
        case formatTextNode: TextNode =>
          val formatValues = formatNode.asText.split("""\|""")
          if (formatValues.length != 2) {
            Right((None, Array(MetadataWarning(propertyPath, s"invalid_boolean_format '$formatNode'"))))
          } else {
            Right((Some(formatTextNode), noWarnings))
          }
        case _ =>
          // Boolean formats should always be textual
          Right((None, Array(MetadataWarning(propertyPath, s"invalid_boolean_format '$formatNode'"))))
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
              Right((Some(new TextNode(format.get)), noWarnings))
            } else {
              Right((None, Array(MetadataWarning(propertyPath, s"invalid_date_format '$dateFormatString'"))))
            }
          } catch {
            case _: DateFormatError =>
              Right((None, Array(MetadataWarning(propertyPath, s"invalid_date_format '$dateFormatString'"))))
          }
        case _ =>
          Right((None, Array(MetadataWarning(propertyPath, s"invalid_date_format '$formatNode'"))))
      }
    } else {
      Left(MetadataError(s"Unhandled format node ${formatNode.toPrettyString}", propertyPath))
    }
  }


  @tailrec
  private def normaliseDataTypeFormatNumeric(
                                          formatNode: JsonNode,
                                          propertyPath: PropertyPath
                                        ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    formatNode match {
      case _: TextNode =>
        val formatObjectNode = JsonNodeFactory.instance.objectNode()
        normaliseDataTypeFormatNumeric(
          formatObjectNode.set("pattern", formatNode.deepCopy()), propertyPath
        )
      case formatObjectNode: ObjectNode =>
        Right(normaliseNumericFormatObjectNode(formatObjectNode, propertyPath))
      case _ =>
        Left(
          MetadataError(
            s"Unhandled numeric data type format ${formatNode.toPrettyString}",
            propertyPath
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


  private def normaliseNumericFormatObjectNode(
                                            formatObjectNode: ObjectNode,
                                            propertyPath: PropertyPath
                                          ): (ObjectNode, MetadataWarnings) = {
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
      case Right(_) => (formatObjectNode, noWarnings)
      case Left(err) =>
        val formatNodeWithoutPattern = formatObjectNode.deepCopy()
        formatNodeWithoutPattern.remove("pattern")

        (
          formatNodeWithoutPattern,
          Array(MetadataWarning(propertyPath, s"invalid_number_format - ${err.message}"))
        )
    }
  }

  private def initialDataTypePropertyNormalisation(
                                            value: JsonNode,
                                            baseUrl: String,
                                            lang: String,
                                            propertyPath: PropertyPath
                                          ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    value match {
      case dataTypeObjectNode: ObjectNode =>
        normaliseDataTypeObject(dataTypeObjectNode, baseUrl, lang, propertyPath)
      case x: TextNode if XsdDataTypes.types.contains(x.asText()) =>
        Right(
          (
            objectMapper.createObjectNode
              .put("@id", XsdDataTypes.types(x.asText())),
            noWarnings
          )
        )
      case _: TextNode =>
        Right(
          (
            objectMapper.createObjectNode
              .put("@id", XsdDataTypes.types("string")),
            Array(MetadataWarning(propertyPath, invalidValueWarning))
          )
        )
      case _ =>
        // If the supplied value of an object property is not a string or object (eg if it is an integer),
        // compliant applications MUST issue a warning and proceed as if the property had been specified as an
        // object with no properties.
        Right(
          (
            JsonNodeFactory.instance.objectNode(),
            Array(MetadataWarning(propertyPath, invalidValueWarning))
          )
        )
    }
  }

  private def normaliseDataTypeObjectIdNode(
                                         baseUrl: String,
                                         lang: String,
                                         valueNode: JsonNode,
                                         propertyPath: PropertyPath
                                       ): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    val idValue = valueNode.asText()
    if (XsdDataTypes.types.values.toList.contains(idValue)) {
      Left(
        MetadataError(
          s"datatype @id must not be the id of a built-in datatype ($idValue)", propertyPath
        )
      )
    } else {
      normaliseUrlLinkProperty(PropertyType.Common)(
        valueNode,
        baseUrl,
        lang,
        propertyPath
      ).map({
        case (linkNode, warns@Array(), _) => (Some(linkNode), warns)
        case (_, warns, _) => (None, warns)
      })
    }
  }

  private def normaliseDataTypeObject(
                                   objectNode: ObjectNode,
                                   baseUrl: String,
                                   lang: String,
                                   propertyPath: PropertyPath
                                 ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    objectNode.fields.asScala
      .map(keyAndValue => {
        val propertyName = keyAndValue.getKey
        val valueNode = keyAndValue.getValue
        val localPropertyPath = propertyPath :+ propertyName

        propertyName match {
          case "@id" =>
            normaliseDataTypeObjectIdNode(baseUrl, lang, valueNode, localPropertyPath)
              .map("@id" +: _)
          case "base" =>
            val baseValue = valueNode.asText()
            if (XsdDataTypes.types.contains(baseValue)) {
              Right(
                (
                  "base",
                  Some(new TextNode(XsdDataTypes.types(baseValue))),
                  noWarnings
                )
              )
            } else {
              Right(
                (
                  "base",
                  Some(new TextNode(XsdDataTypes.types("string"))),
                  Array(MetadataWarning(localPropertyPath, "invalid_datatype_base"))
                )
              )
            }
          case _ =>
            Right(
              (propertyName, Some(valueNode), noWarnings)
            ) // todo: Is this right?
        }
      })
      .toObjectNodeAndWarnings
      .map({
        case (objectNode, warnings) =>
          // Make sure that the `base` node is set.
          objectNode
            .getMaybeNode("base")
            .map(_ => (objectNode, warnings))
            .getOrElse(
              (
                objectNode
                  .deepCopy()
                  .set("base", new TextNode(XsdDataTypes.types("string"))),
                warnings
              )
            )
      })
  }

  private def normaliseDataTypeMinMaxValues(
                                         inputs: (ObjectNode, MetadataWarnings),
                                         propertyPath: PropertyPath
                                       ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    val (dataTypeNode, warnings) = inputs
    dataTypeNode
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          normaliseDataTypeMinMaxValuesForBaseType(
            dataTypeNode,
            warnings,
            baseDataTypeNode.asText,
            propertyPath :+ "base"
          )
        case baseNode =>
          Left(
            MetadataError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}",
              propertyPath :+ "base"
            )
          )
      })
      .getOrElse(Right(inputs))
  }

  private def normaliseDataTypeMinMaxValuesForBaseType(
                                                    dataTypeNode: ObjectNode,
                                                    existingStringWarnings: MetadataWarnings,
                                                    baseDataType: String,
                                                    propertyPath: PropertyPath
                                                  ): ParseResult[(ObjectNode, MetadataWarnings)] = {
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
      normaliseMinMaxRanges(
        dataTypeNode,
        baseDataType,
        minimumNode,
        maximumNode,
        minInclusiveNode,
        minExclusiveNode,
        maxInclusiveNode,
        maxExclusiveNode,
        existingStringWarnings,
        propertyPath
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
            "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types",
            propertyPath
          )
        )
      } else {
        Right((dataTypeNode, existingStringWarnings))
      }
    }
  }

  private def normaliseMinMaxRanges(
                                 dataTypeNode: ObjectNode,
                                 baseDataType: String,
                                 minimumNode: Option[JsonNode],
                                 maximumNode: Option[JsonNode],
                                 minInclusiveNode: Option[JsonNode],
                                 minExclusiveNode: Option[JsonNode],
                                 maxInclusiveNode: Option[JsonNode],
                                 maxExclusiveNode: Option[JsonNode],
                                 stringWarnings: MetadataWarnings,
                                 propertyPath: PropertyPath
                               ): ParseResult[(ObjectNode, MetadataWarnings)] = {

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
            s"datatype cannot specify both minimum/minInclusive ($minI) and minExclusive ($minE)",
            propertyPath
          )
        )
      case (_, _, Some(maxI), Some(maxE)) =>
        Left(
          MetadataError(
            s"datatype cannot specify both maximum/maxInclusive ($maxI) and maxExclusive ($maxE)",
            propertyPath
          )
        )
      case _ =>
        if (
          Constants.NumericFormatDataTypes.contains(baseDataType)
        ) {
          normaliseMinMaxNumericRanges(
            dataTypeNode,
            stringWarnings,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive,
            propertyPath
          )
        } else if (
          Constants.DateFormatDataTypes.contains(baseDataType)
        ) {
          normaliseMinMaxDateTimeRanges(
            dataTypeNode,
            stringWarnings,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive,
            propertyPath
          )
        } else {
          Left(
            MetadataError(
              s"Base data type was neither numeric note date/time - $baseDataType", propertyPath
            )
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

  private def normaliseMinMaxNumericRanges(
                                        dataTypeNode: ObjectNode,
                                        stringWarnings: MetadataWarnings,
                                        minInclusive: Option[String],
                                        maxInclusive: Option[String],
                                        minExclusive: Option[String],
                                        maxExclusive: Option[String],
                                        propertyPath: PropertyPath
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
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)",
            propertyPath
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI >= maxE =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)",
            propertyPath
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE > maxE =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)",
            propertyPath
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE >= maxI =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)",
            propertyPath
          )
        )
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }

  private def normaliseDataTypeLengths(
                                    inputs: (ObjectNode, MetadataWarnings),
                                    propertyPath: PropertyPath
                                  ): ParseResult[(ObjectNode, MetadataWarnings)] = {
    val (dataTypeNode: ObjectNode, warnings) = inputs

    dataTypeNode
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          normaliseDataTypeWithBase(
            dataTypeNode,
            baseDataTypeNode.asText,
            warnings,
            propertyPath :+ "base"
          )
        case baseNode =>
          Left(
            MetadataError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}",
              propertyPath :+ "base"
            )
          )
      })
      .getOrElse(Right(inputs))
  }

  private def normaliseDataTypeWithBase(
                                     dataTypeNode: ObjectNode,
                                     baseDataType: String,
                                     existingWarnings: MetadataWarnings,
                                     propertyPath: PropertyPath
                                   ): ParseResult[(ObjectNode, MetadataWarnings)] = {
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
            s"datatype length (${lengthNode.map(_.asInt).get}) cannot be less than minLength (${minLengthNode.map(_.asInt).get})",
            propertyPath
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
            })",
            propertyPath
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
            })",
            propertyPath
          )
        )
      } else {
        Right((dataTypeNode, existingWarnings))
      }
    } else {
      // length, minLength and maxLength are only permitted on String and Binary data types.
      if (lengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a length facet",
            propertyPath :+ "length"
          )
        )
      } else if (minLengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a minLength facet",
            propertyPath :+ "minLength"
          )
        )
      } else if (maxLengthNode.isDefined) {
        Left(
          MetadataError(
            s"Data types based on $baseDataType cannot have a maxLength facet",
            propertyPath :+ "maxLength"
          )
        )
      } else {
        Right((dataTypeNode, existingWarnings))
      }
    }
  }

  private def normaliseMinMaxDateTimeRanges(
                                         dataTypeNode: ObjectNode,
                                         stringWarnings: MetadataWarnings,
                                         minInclusive: Option[String],
                                         maxInclusive: Option[String],
                                         minExclusive: Option[String],
                                         maxExclusive: Option[String],
                                         propertyPath: PropertyPath
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
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)",
            propertyPath
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI.getMillis >= maxE.getMillis =>
        Left(
          MetadataError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)",
            propertyPath
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE.getMillis > maxE.getMillis =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)",
            propertyPath
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE.getMillis >= maxI.getMillis =>
        Left(
          MetadataError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)",
            propertyPath
          )
        )
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }
}
