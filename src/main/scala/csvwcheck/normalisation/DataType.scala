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
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedObjectProperties, MetadataWarnings, NormContext, Normaliser, PropertyPath, invalidValueWarning, noWarnings, normaliseUrlLinkProperty, parseNodeAsText}
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import org.joda.time.DateTime
import shapeless.syntax.std.tuple.productTupleOps

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

object DataType {
  val normalisers: Map[String, Normaliser] = Map(
    "@id" ->normaliseDataTypeObjectIdNode(PropertyType.DataType),
    "base" -> normaliseDataTypeBase(PropertyType.DataType),

    // The remaining properties are permitted, but are parsed collectively inside `normaliseDataTypeProperty`.
    "format" -> Utils.normaliseDoNothing(PropertyType.DataType),

    "minimum" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "maximum" -> Utils.normaliseDoNothing(PropertyType.DataType),

    "minInclusive" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "maxInclusive" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "minExclusive" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "maxExclusive" -> Utils.normaliseDoNothing(PropertyType.DataType),

    "length" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "minLength" -> Utils.normaliseDoNothing(PropertyType.DataType),
    "maxLength" -> Utils.normaliseDoNothing(PropertyType.DataType),
  )

  def normaliseDataType(
                             csvwPropertyType: PropertyType.Value
                           ): Normaliser = context =>
    initialDataTypePropertyNormalisation(context)
      .flatMap({ case (dataTypeContext, warnings) =>
        for {
          _ <- normaliseDataTypeLengths(dataTypeContext)
          _ <- normaliseDataTypeMinMaxValues(dataTypeContext)
          normalisedDataTypeNodeWithWarnings <- normaliseDataTypeFormat(dataTypeContext)
        } yield {
          val (normalisedNode, newWarnings) = normalisedDataTypeNodeWithWarnings
          (normalisedNode, warnings ++ newWarnings, csvwPropertyType)
        }
      })

  private def normaliseDataTypeFormat(dataTypeContext: NormContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] =
    dataTypeContext.node
      .getMaybeNode("format")
      .map(formatNode => {
        val formatNodeContext = dataTypeContext.toChild(formatNode, "format")
        for {
          baseDataType <- Utils.parseNodeAsText(dataTypeContext.node.get("base"))
          dataTypeFormatNodeAndWarnings <- normaliseDataTypeFormat(formatNodeContext, baseDataType)
        } yield {
          val (formatNodeReplacement, newWarnings) =
            dataTypeFormatNodeAndWarnings

          val parsedDataTypeNode = formatNodeReplacement match {
            case Some(newNode) =>
              dataTypeContext.node.deepCopy().set("format", newNode)
            case None =>
              val modifiedDataTypeNode = dataTypeContext.node
                .deepCopy()
              modifiedDataTypeNode.remove("format")

              modifiedDataTypeNode
          }
          (parsedDataTypeNode, newWarnings)
        }
      })
      .getOrElse(Right((dataTypeContext.node, noWarnings)))

  private def normaliseDataTypeFormat(
                                       formatContext: NormContext[JsonNode],
                                       baseDataType: String
                                 ): ParseResult[(Option[JsonNode], MetadataWarnings)] = {
    if (Constants.RegExpFormatDataTypes.contains(baseDataType)) {
      parseNodeAsText(formatContext.node)
        .flatMap(regExFormat =>
          validateRegEx(regExFormat) match {
            case Right(()) => Right((Some(new TextNode(regExFormat)), noWarnings))
            case Left(e) =>
              Right(
                (None, Array(formatContext.makeWarning(s"invalid_regex '$regExFormat' - ${e.getMessage}")))
              )
          }
        )
    } else if (
      Constants.NumericFormatDataTypes.contains(baseDataType)
    ) {
      normaliseDataTypeFormatNumeric(formatContext)
        .map({
          case (parsedNode, warnings) =>
            (Some(parsedNode), warnings)
        })
    } else if (baseDataType == "http://www.w3.org/2001/XMLSchema#boolean") {
      formatContext.node match {
        case formatTextNode: TextNode =>
          val formatValues = formatTextNode.asText.split("""\|""")
          if (formatValues.length != 2) {
            Right((None, Array(formatContext.makeWarning(s"invalid_boolean_format '$formatTextNode'"))))
          } else {
            Right((Some(formatTextNode), noWarnings))
          }
        case formatNode =>
          // Boolean formats should always be textual
          Right((None, Array(formatContext.makeWarning(s"invalid_boolean_format '$formatNode'"))))
      }
    } else if (
      Constants.DateFormatDataTypes.contains(baseDataType)
    ) {
      formatContext.node match {
        case formatTextNode: TextNode =>
          val dateFormatString = formatTextNode.asText()
          try {
            val format = DateFormat(Some(dateFormatString), baseDataType).format
            if (format.isDefined) {
              Right((Some(new TextNode(format.get)), noWarnings))
            } else {
              Right((None, Array(formatContext.makeWarning(s"invalid_date_format '$dateFormatString'"))))
            }
          } catch {
            case _: DateFormatError =>
              Right((None, Array(formatContext.makeWarning(s"invalid_date_format '$dateFormatString'"))))
          }
        case formatNode =>
          Right((None, Array(formatContext.makeWarning(s"invalid_date_format '$formatNode'"))))
      }
    } else {
      Left(formatContext.makeError(s"Unhandled format node ${formatContext.node.toPrettyString}"))
    }
  }


  @tailrec
  private def normaliseDataTypeFormatNumeric(formatContext: NormContext[JsonNode]): ParseResult[(ObjectNode, MetadataWarnings)] = {
    formatContext.node match {
      case _: TextNode =>
        val formatObjectNode = JsonNodeFactory.instance.objectNode()
        normaliseDataTypeFormatNumeric(
          formatContext.withNode(
            formatObjectNode.set("pattern", formatContext.node.deepCopy())
          )
        )
      case formatObjectNode: ObjectNode =>
        Right(normaliseNumericFormatObjectNode(formatContext.withNode(formatObjectNode)))
      case _ =>
        Left(
          formatContext.makeError(
            s"Unhandled numeric data type format ${formatContext.node.toPrettyString}"
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


  private def normaliseNumericFormatObjectNode(formatContext: NormContext[ObjectNode]): (ObjectNode, MetadataWarnings) = {
    def parseMaybeStringAt(propertyName: String): ParseResult[Option[String]] =
      formatContext.node
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
      case Right(_) => (formatContext.node, noWarnings)
      case Left(err) =>
        val formatNodeWithoutPattern = formatContext.node.deepCopy()
        formatNodeWithoutPattern.remove("pattern")

        (
          formatNodeWithoutPattern,
          Array(formatContext.makeWarning(s"invalid_number_format - ${err.message}"))
        )
    }
  }

  private def initialDataTypePropertyNormalisation(dataTypeContext: NormContext[JsonNode]): ParseResult[(NormContext[ObjectNode], MetadataWarnings)] = {
    dataTypeContext.node match {
      case dataTypeObjectNode: ObjectNode => normaliseDataTypeObject(dataTypeContext.withNode(dataTypeObjectNode))
      case x: TextNode if XsdDataTypes.types.contains(x.asText()) =>
        Right(
          (
            dataTypeContext.withNode(
              objectMapper.createObjectNode
                .put("@id", XsdDataTypes.types(x.asText()))
            ),
            noWarnings
          )
        )
      case _: TextNode =>
        Right(
          (
            dataTypeContext.withNode(
              objectMapper.createObjectNode
                .put("@id", XsdDataTypes.types("string"))
            ),
            Array(dataTypeContext.makeWarning(invalidValueWarning))
          )
        )
      case _ =>
        // If the supplied value of an object property is not a string or object (eg if it is an integer),
        // compliant applications MUST issue a warning and proceed as if the property had been specified as an
        // object with no properties.
        Right(
          (
            dataTypeContext.withNode(
              JsonNodeFactory.instance.objectNode()
            ),
            Array(dataTypeContext.makeWarning(invalidValueWarning))
          )
        )
    }
  }

  private def normaliseDataTypeObjectIdNode(propertyType: PropertyType.Value): Normaliser = (context: NormContext[JsonNode]) =>
    parseNodeAsText(context.node)
      .flatMap(idValue => {
        if (XsdDataTypes.types.values.toList.contains(idValue)) {
          Left(
            context.makeError(
              s"datatype @id must not be the id of a built-in datatype ($idValue)"
            )
          )
        } else {
          normaliseUrlLinkProperty(propertyType)(context)
        }
      })

  private def normaliseDataTypeObject(context: NormContext[ObjectNode]): ParseResult[(NormContext[ObjectNode], MetadataWarnings)] = {
    Utils.normaliseObjectNode(normalisers, context)
      .map({
        case (objectNode, warnings) =>
          // Make sure that the `base` node is set.
          objectNode
            .getMaybeNode("base")
            .map(_ => (context.withNode(objectNode), warnings))
            .getOrElse(
              (
                context.withNode(
                  objectNode
                    .deepCopy()
                    .set("base", new TextNode(XsdDataTypes.types("string"))),
                ),
                warnings
              )
            )
      })
  }

  private def normaliseDataTypeBase(propertyType: PropertyType.Value): Normaliser = context => {
    parseNodeAsText(context.node)
      .map(baseValue => {
        if (XsdDataTypes.types.contains(baseValue)) {
          (
            new TextNode(XsdDataTypes.types(baseValue)),
            noWarnings,
            propertyType
          )
        } else {
          (
            new TextNode(XsdDataTypes.types("string")),
            Array(context.makeWarning("invalid_datatype_base")),
            propertyType
          )
        }
      })
  }

  private def normaliseDataTypeMinMaxValues(dataTypeContext: NormContext[ObjectNode]): ParseResult[Unit] = {
    dataTypeContext.node
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          normaliseDataTypeMinMaxValuesForBaseType(
            dataTypeContext,
            baseDataTypeNode.asText
          )
        case baseNode =>
          Left(
            dataTypeContext.makeError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(()))
  }

  private def normaliseDataTypeMinMaxValuesForBaseType(
                                                    dataTypeContext: NormContext[ObjectNode],
                                                    baseDataType: String
                                                  ): ParseResult[Unit] = {
    val dataTypeNode = dataTypeContext.node

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
        dataTypeContext,
        baseDataType,
        minimumNode,
        maximumNode,
        minInclusiveNode,
        minExclusiveNode,
        maxInclusiveNode,
        maxExclusiveNode
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
          dataTypeContext.makeError(
            "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types"
          )
        )
      } else {
        Right(())
      }
    }
  }

  private def normaliseMinMaxRanges(
                                 dataTypeContext: NormContext[ObjectNode],
                                 baseDataType: String,
                                 minimumNode: Option[JsonNode],
                                 maximumNode: Option[JsonNode],
                                 minInclusiveNode: Option[JsonNode],
                                 minExclusiveNode: Option[JsonNode],
                                 maxInclusiveNode: Option[JsonNode],
                                 maxExclusiveNode: Option[JsonNode]
                               ): ParseResult[Unit] = {

    val dataTypeNode = dataTypeContext.node

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
          dataTypeContext.makeError(
            s"datatype cannot specify both minimum/minInclusive ($minI) and minExclusive ($minE)"
          )
        )
      case (_, _, Some(maxI), Some(maxE)) =>
        Left(
          dataTypeContext.makeError(
            s"datatype cannot specify both maximum/maxInclusive ($maxI) and maxExclusive ($maxE)"
          )
        )
      case _ =>
        if (
          Constants.NumericFormatDataTypes.contains(baseDataType)
        ) {
          normaliseMinMaxNumericRanges(
            dataTypeContext,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive
          )
        } else if (
          Constants.DateFormatDataTypes.contains(baseDataType)
        ) {
          normaliseMinMaxDateTimeRanges(
            dataTypeContext,
            minInclusive,
            maxInclusive,
            minExclusive,
            maxExclusive
          )
        } else {
          Left(
            dataTypeContext.makeError(
              s"Base data type was neither numeric note date/time - $baseDataType"
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
                                        dataTypeContext: NormContext[ObjectNode],
                                        minInclusive: Option[String],
                                        maxInclusive: Option[String],
                                        minExclusive: Option[String],
                                        maxExclusive: Option[String]
                                      ): ParseResult[Unit] = {
    (
      minInclusive.map(BigDecimal(_)),
      minExclusive.map(BigDecimal(_)),
      maxInclusive.map(BigDecimal(_)),
      maxExclusive.map(BigDecimal(_))
    ) match {
      case (Some(minI), _, Some(maxI), _) if minI > maxI =>
        Left(
          dataTypeContext.makeError(
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI >= maxE =>
        Left(
          dataTypeContext.makeError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE > maxE =>
        Left(
          dataTypeContext.makeError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE >= maxI =>
        Left(
          dataTypeContext.makeError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case _ => Right(())
    }
  }

  private def normaliseDataTypeLengths(dataTypeContext: NormContext[ObjectNode]): ParseResult[Unit] = {
    dataTypeContext.node
      .getMaybeNode("base")
      .map({
        case baseDataTypeNode: TextNode =>
          normaliseDataTypeWithBase(
            dataTypeContext,
            baseDataTypeNode.asText
          )
        case baseNode =>
          Left(
            dataTypeContext.makeError(
              s"Unexpected base data type value: ${baseNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(()))
  }

  private def normaliseDataTypeWithBase(dataTypeContext: NormContext[ObjectNode], baseDataType: String): ParseResult[Unit] = {
    val dataTypeNode = dataTypeContext.node

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
          dataTypeContext.makeError(
            s"datatype length (${lengthNode.map(_.asInt).get}) cannot be less than minLength (${minLengthNode.map(_.asInt).get})"
          )
        )
      } else if (
        lengthNode.exists(len =>
          maxLengthNode.exists(maxLen => len.asInt > maxLen.asInt)
        )
      ) {
        Left(
          dataTypeContext.makeError(
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
          dataTypeContext.makeError(
            s"datatype minLength (${minLengthNode.map(_.asInt)}) cannot be more than maxLength (${
              maxLengthNode
                .map(_.asInt)
            })"
          )
        )
      } else {
        Right(())
      }
    } else {
      // length, minLength and maxLength are only permitted on String and Binary data types.
      if (lengthNode.isDefined) {
        Left(
          dataTypeContext.makeError(
            s"Data types based on $baseDataType cannot have a length facet"
          )
        )
      } else if (minLengthNode.isDefined) {
        Left(
          dataTypeContext.makeError(
            s"Data types based on $baseDataType cannot have a minLength facet"
          )
        )
      } else if (maxLengthNode.isDefined) {
        Left(
          dataTypeContext.makeError(
            s"Data types based on $baseDataType cannot have a maxLength facet"
          )
        )
      } else {
        Right(())
      }
    }
  }

  private def normaliseMinMaxDateTimeRanges(
                                         dataTypeContext: NormContext[ObjectNode],
                                         minInclusive: Option[String],
                                         maxInclusive: Option[String],
                                         minExclusive: Option[String],
                                         maxExclusive: Option[String]
                                       ): ParseResult[Unit] = {
    (
      minInclusive.map(DateTime.parse),
      minExclusive.map(DateTime.parse),
      maxInclusive.map(DateTime.parse),
      maxExclusive.map(DateTime.parse)
    ) match {
      case (Some(minI), _, Some(maxI), _) if minI.getMillis > maxI.getMillis =>
        Left(
          dataTypeContext.makeError(
            s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case (Some(minI), _, _, Some(maxE)) if minI.getMillis >= maxE.getMillis =>
        Left(
          dataTypeContext.makeError(
            s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), _, Some(maxE)) if minE.getMillis > maxE.getMillis =>
        Left(
          dataTypeContext.makeError(
            s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
          )
        )
      case (_, Some(minE), Some(maxI), _) if minE.getMillis >= maxI.getMillis =>
        Left(
          dataTypeContext.makeError(
            s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
          )
        )
      case _ => Right(())
    }
  }
}
