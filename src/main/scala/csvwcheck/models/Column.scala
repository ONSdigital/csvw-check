package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import com.typesafe.scalalogging.Logger
import csvwcheck.propertyparser.Utils.{parseNodeAsBool, parseNodeAsInt, parseNodeAsText}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, ErrorWithoutContext, MetadataError}
import csvwcheck.models.Column._
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.traits.LoggerExtensions.LogDebugException
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import csvwcheck.models
import csvwcheck.propertyparser.Constants.undefinedLanguage
import csvwcheck.propertyparser.PropertyParser
import org.joda.time.{DateTime, DateTimeZone}
import shapeless.syntax.std.tuple.productTupleOps

import java.math.BigInteger
import java.time.{LocalDateTime, Month, ZoneId, ZonedDateTime}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.math.BigInt.javaBigInteger2bigInt
import scala.util.matching.Regex

object Column {
  val xmlSchema = "http://www.w3.org/2001/XMLSchema#"
  val datatypeDefaultValue: ObjectNode = JsonNodeFactory.instance
    .objectNode()
    .set("@id", new TextNode(s"${xmlSchema}string"))
  val rdfSyntaxNs = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  // https://www.w3.org/TR/xmlschema11-2/#float, https://www.w3.org/TR/xmlschema11-2/#double
  val validDoubleDatatypeRegex, validFloatDatatypeRegex =
    "(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?|(\\+|-)?INF|NaN".r
  // https://www.w3.org/TR/xmlschema11-2/#duration
  val validDurationRegex: Regex =
    "-?P((([0-9]+Y([0-9]+M)?([0-9]+D)?|([0-9]+M)([0-9]+D)?|([0-9]+D))(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S)))?)|(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S))))".r
  val validDayTimeDurationRegex: Regex =
    "-?P(([0-9]+D(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S)))?)|(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S))))".r
  val validYearMonthDurationRegex: Regex =
    """-?P([0-9]+Y([0-9]+M)?|([0-9]+M))""".r
  val unsignedLongMaxValue: BigInt = BigInt("18446744073709551615")
  private val validDecimalDatatypeRegex =
    "(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)".r
  private val validIntegerRegex = "[\\-+]?[0-9]+".r
  private val validLongDatatypeRegex = "[\\-+]?[0-9]+".r

  def fromJson(
      columnOrdinal: Int,
      columnNode: ObjectNode,
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[(Column, Array[ErrorWithoutContext])] = {
    for {
      parsedColumnProperties <- partitionAndValidateColumnPropertiesByType(
        columnNode,
        columnOrdinal,
        baseUrl,
        lang,
        inheritedProperties
      )
      dataTypeObjectNode <- getDatatypeOrDefault(
        parsedColumnProperties.inherited
      )
      titles <- parseLangTitles(parsedColumnProperties.column.get("titles"))
      nullParam <- getNullParam(parsedColumnProperties.inherited)
      urls <- parseUrls(parsedColumnProperties)
      lengthRestrictions <- parseLengthRestrictions(dataTypeObjectNode)
      numericAndDateRangeRestrictions <- parseNumericAndDateRangeRestrictions(
        dataTypeObjectNode
      )
      baseDataType <- parseBaseDataType(dataTypeObjectNode)
      name <- parseTextProperty(parsedColumnProperties.column, "name")
      default <- parseTextProperty(parsedColumnProperties.inherited, "default")
      format <-
        dataTypeObjectNode
          .getMaybeNode("format")
          .map(getFormat(_).map(Some(_)))
          .getOrElse(Right(None))
      lang <- parseTextProperty(parsedColumnProperties.inherited, "lang")
      id <- parseTextProperty(parsedColumnProperties.inherited, "id")
      ordered <-
        parseBooleanProperty(parsedColumnProperties.inherited, "ordered")
      required <-
        parseBooleanProperty(parsedColumnProperties.inherited, "required")
      separator <-
        parseTextProperty(parsedColumnProperties.inherited, "separator")
      suppressOutput <-
        parseBooleanProperty(parsedColumnProperties.column, "suppressOutput")
      textDirection <-
        parseTextProperty(parsedColumnProperties.inherited, "textDirection")
      virtual <- parseBooleanProperty(parsedColumnProperties.column, "virtual")
    } yield {
      val column = new Column(
        annotations = parsedColumnProperties.annotation,
        baseDataType = baseDataType,
        columnOrdinal = columnOrdinal,
        default = default.getOrElse(""),
        format = format,
        name = name,
        id = id,
        lengthRestrictions = lengthRestrictions,
        lang = lang.getOrElse(undefinedLanguage),
        nullParam = nullParam,
        numericAndDateRangeRestrictions = numericAndDateRangeRestrictions,
        ordered = ordered.getOrElse(false),
        required = required.getOrElse(false),
        separator = separator,
        suppressOutput = suppressOutput.getOrElse(false),
        titleValues = titles,
        textDirection = textDirection.getOrElse("inherit"),
        urls = urls,
        virtual = virtual.getOrElse(false)
      )

      (column, parsedColumnProperties.warnings)
    }
  }

  def getNullParam(
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[Array[String]] = {
    inheritedProperties
      .get("null")
      .map({
        case a: ArrayNode =>
          var nullParamsToReturn = Array[String]()
          val nullParams = Array.from(a.elements.asScala)
          for (np <- nullParams)
            nullParamsToReturn :+= np.asText()

          Right(nullParamsToReturn)
        case s: TextNode => Right(Array[String](s.asText()))
        case nullNode =>
          Left(
            MetadataError(
              s"unexpected value for null property: ${nullNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(Array[String]("")))
  }

  def parseLangTitles(
      titles: Option[JsonNode]
  ): ParseResult[Map[String, Array[String]]] = {
    titles
      .map({
        case titlesObjectNode: ObjectNode =>
          titlesObjectNode.getKeysAndValues
            .foldLeft[ParseResult[Map[String, Array[String]]]](Right(Map()))({
              case (err @ Left(_), _) => err
              case (Right(titlesMap), (lang, values: ArrayNode)) =>
                Right(
                  titlesMap + (lang -> Array
                    .from(values.elements().asScala.map(_.asText())))
                )
              case (_, (_, valuesNode)) =>
                Left(
                  MetadataError(
                    s"Unexpected language titles value: ${valuesNode.toPrettyString}"
                  )
                )
            })
        case valuesNode =>
          Left(
            MetadataError(
              s"Unexpected language mapping: ${valuesNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(Map()))
  }

  def parseBaseDataType(dataTypeObjectNode: ObjectNode): ParseResult[String] =
    dataTypeObjectNode
      .getMaybeNode("base")
      .orElse(dataTypeObjectNode.getMaybeNode("@id"))
      .map(parseNodeAsText(_))
      .getOrElse(
        Left(MetadataError("datatype object has neither `base` nor `@id`"))
      )

  def partitionAndValidateColumnPropertiesByType(
      columnDesc: ObjectNode,
      columnOrdinal: Int,
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[ParsedColumnProperties] = {
    val initialAccumulator: ParseResult[ParsedColumnProperties] = Right(
      ParsedColumnProperties(inherited = inheritedProperties)
    )

    columnDesc.getKeysAndValues
      .map({
        case ("@type", textNode: TextNode) if textNode.asText != "Column" =>
          Left(
            MetadataError(
              s"columns[$columnOrdinal].@type, @type of column is not 'Column'"
            )
          )
        case (propertyName, value) =>
          PropertyParser
            .parseJsonProperty(propertyName, value, baseUrl, lang)
            .map(propertyName +: _)
      })
      .foldLeft(initialAccumulator)({
        case (err @ Left(_), _) => err
        case (_, Left(newErr))  => Left(newErr)
        case (
              Right(parsedProperties),
              Right((propertyName, parsedValue, stringWarnings, propertyType))
            ) =>
          val mappedWarnings = stringWarnings.map(
            ErrorWithoutContext(
              _,
              s"$propertyName: ${columnDesc.get(propertyName).toPrettyString}"
            )
          )

          val partitionedProperties = propertyType match {
            case PropertyType.Inherited =>
              parsedProperties.copy(
                inherited =
                  parsedProperties.inherited + (propertyName -> parsedValue),
                warnings = parsedProperties.warnings ++ mappedWarnings
              )
            case PropertyType.Annotation =>
              parsedProperties.copy(
                annotation =
                  parsedProperties.annotation + (propertyName -> parsedValue),
                warnings = parsedProperties.warnings ++ mappedWarnings
              )
            case PropertyType.Common | PropertyType.Column =>
              parsedProperties.copy(
                column =
                  parsedProperties.column + (propertyName -> parsedValue),
                warnings = parsedProperties.warnings ++ mappedWarnings
              )
            case _ =>
              parsedProperties.copy(
                warnings =
                  parsedProperties.warnings ++ mappedWarnings :+ ErrorWithoutContext(
                    s"invalid_property",
                    s"column: $propertyName"
                  )
              )
          }

          Right(partitionedProperties)
      })
  }

  private def parseUrls(
      parsedColumnProperties: ParsedColumnProperties
  ): ParseResult[Urls] =
    for {
      aboutUrl <-
        parseTextProperty(parsedColumnProperties.inherited, "aboutUrl")
      propertyUrl <-
        parseTextProperty(parsedColumnProperties.inherited, "propertyUrl")
      valueUrl <-
        parseTextProperty(parsedColumnProperties.inherited, "valueUrl")
    } yield Urls(
      aboutUrl = aboutUrl,
      propertyUrl = propertyUrl,
      valueUrl = valueUrl
    )

  private def parseTextProperty(
      propertiesMap: Map[String, JsonNode],
      property: String
  ): ParseResult[Option[String]] =
    propertiesMap
      .get(property)
      .flatMap(filterOutNullOrMissingNodes)
      .map(n => parseNodeAsText(n).map(Some(_)))
      .getOrElse(Right(None))

  private def filterOutNullOrMissingNodes(n: JsonNode): Option[JsonNode] =
    if (n.isMissingNode || n.isNull)
      None
    else
      Some(n)

  private def parseNumericAndDateRangeRestrictions(
      dataTypeObjectNode: ObjectNode
  ): ParseResult[NumericAndDateRangeRestrictions] =
    for {
      minInclusive <-
        dataTypeObjectNode
          .getMaybeNode("minInclusive")
          .map(v => parseNodeAsText(v, true).map(Some(_)))
          .getOrElse(Right(None))
      maxInclusive <-
        dataTypeObjectNode
          .getMaybeNode("maxInclusive")
          .map(v => parseNodeAsText(v, true).map(Some(_)))
          .getOrElse(Right(None))
      minExclusive <-
        dataTypeObjectNode
          .getMaybeNode("minExclusive")
          .map(v => parseNodeAsText(v, true).map(Some(_)))
          .getOrElse(Right(None))
      maxExclusive <-
        dataTypeObjectNode
          .getMaybeNode("maxExclusive")
          .map(v => parseNodeAsText(v, true).map(Some(_)))
          .getOrElse(Right(None))
    } yield NumericAndDateRangeRestrictions(
      minInclusive = minInclusive,
      maxInclusive = maxInclusive,
      minExclusive = minExclusive,
      maxExclusive = maxExclusive
    )

  private def parseLengthRestrictions(
      dataTypeObjectNode: ObjectNode
  ): ParseResult[LengthRestrictions] =
    for {
      length <-
        dataTypeObjectNode
          .getMaybeNode("length")
          .map(v => parseNodeAsInt(v).map(Some(_)))
          .getOrElse(Right(None))
      minLength <-
        dataTypeObjectNode
          .getMaybeNode("minLength")
          .map(v => parseNodeAsInt(v).map(Some(_)))
          .getOrElse(Right(None))
      maxLength <-
        dataTypeObjectNode
          .getMaybeNode("maxLength")
          .map(v => parseNodeAsInt(v).map(Some(_)))
          .getOrElse(Right(None))
    } yield LengthRestrictions(
      length = length,
      minLength = minLength,
      maxLength = maxLength
    )

  private def parseBooleanProperty(
      propertiesMap: Map[String, JsonNode],
      property: String
  ): ParseResult[Option[Boolean]] =
    propertiesMap
      .get(property)
      .flatMap(filterOutNullOrMissingNodes)
      .map(n => parseNodeAsBool(n).map(Some(_)))
      .getOrElse(Right(None))

  private def getFormat(formatNode: JsonNode): ParseResult[Format] = {
    formatNode match {
      case s: TextNode => Right(Format(Some(s.asText()), None, None))
      case formatObjectNode: ObjectNode =>
        def getMaybeValue(propertyName: String): ParseResult[Option[String]] =
          formatObjectNode
            .getMaybeNode(propertyName)
            .map(parseNodeAsText(_).map(Some(_)))
            .getOrElse(Right(None))

        for {
          pattern <- getMaybeValue("pattern")
          decimalChar <- getMaybeValue("decimalChar")
            .map(_.map(d => d(0)))
          groupChar <- getMaybeValue("groupChar")
            .map(_.map(d => d(0)))
        } yield {
          Format(pattern, decimalChar, groupChar)
        }
      case node =>
        Left(
          MetadataError(
            s"Unexpected format node value: {${node.toPrettyString}"
          )
        )
    }
  }

  private def getDatatypeOrDefault(
      inheritedPropertiesCopy: Map[String, JsonNode]
  ): ParseResult[ObjectNode] =
    inheritedPropertiesCopy
      .get("datatype")
      .map({
        case dataTypeNode: ObjectNode => Right(dataTypeNode)
        case dataTypeNode =>
          Left(
            MetadataError(
              s"Unexpected datatype value: ${dataTypeNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(datatypeDefaultValue))

  def languagesMatch(l1: String, l2: String): Boolean = {
    val languagesMatchOrEitherIsUndefined =
      l1 == l2 || l1 == undefinedLanguage || l2 == undefinedLanguage
    val oneLanguageIsSubClassOfAnother =
      l1.startsWith(s"$l2-") || l2.startsWith(s"$l1-")

    languagesMatchOrEitherIsUndefined || oneLanguageIsSubClassOfAnother
  }

  case class Urls private (
      aboutUrl: Option[String],
      propertyUrl: Option[String],
      valueUrl: Option[String]
  )

  case class LengthRestrictions private (
      length: Option[Int],
      minLength: Option[Int],
      maxLength: Option[Int]
  )

  case class NumericAndDateRangeRestrictions private (
      minInclusive: Option[String],
      maxInclusive: Option[String],
      minExclusive: Option[String],
      maxExclusive: Option[String]
  )

  case class ParsedColumnProperties(
      inherited: Map[String, JsonNode] = Map(),
      annotation: Map[String, JsonNode] = Map(),
      column: Map[String, JsonNode] = Map(),
      warnings: Array[ErrorWithoutContext] = Array()
  )
}

case class Column private (
    columnOrdinal: Int,
    name: Option[String],
    id: Option[String],
    lengthRestrictions: LengthRestrictions,
    numericAndDateRangeRestrictions: NumericAndDateRangeRestrictions,
    baseDataType: String,
    default: String,
    lang: String,
    nullParam: Array[String],
    ordered: Boolean,
    urls: Urls,
    required: Boolean,
    separator: Option[String],
    suppressOutput: Boolean,
    textDirection: String,
    titleValues: Map[String, Array[String]],
    virtual: Boolean,
    format: Option[Format],
    annotations: Map[String, JsonNode]
) {
  lazy val minInclusiveNumeric: Option[BigDecimal] =
    numericAndDateRangeRestrictions.minInclusive.map(BigDecimal(_))
  lazy val maxInclusiveNumeric: Option[BigDecimal] =
    numericAndDateRangeRestrictions.maxInclusive.map(BigDecimal(_))
  lazy val minExclusiveNumeric: Option[BigDecimal] =
    numericAndDateRangeRestrictions.minExclusive.map(BigDecimal(_))
  lazy val maxExclusiveNumeric: Option[BigDecimal] =
    numericAndDateRangeRestrictions.maxExclusive.map(BigDecimal(_))
  lazy val minInclusiveInt: Option[BigInt] = minInclusiveNumeric.map(_.toBigInt)
  lazy val maxInclusiveInt: Option[BigInt] = maxInclusiveNumeric.map(_.toBigInt)
  lazy val minExclusiveInt: Option[BigInt] = minExclusiveNumeric.map(_.toBigInt)
  lazy val maxExclusiveInt: Option[BigInt] = maxExclusiveNumeric.map(_.toBigInt)
  lazy val minInclusiveDateTime: Option[ZonedDateTime] =
    numericAndDateRangeRestrictions.minInclusive.map(v =>
      mapJodaDateTimeToZonedDateTime(DateTime.parse(v))
    )

  DateTimeZone.setDefault(DateTimeZone.UTC)
  lazy val maxInclusiveDateTime: Option[ZonedDateTime] =
    numericAndDateRangeRestrictions.maxInclusive.map(v =>
      mapJodaDateTimeToZonedDateTime(DateTime.parse(v))
    )
  lazy val minExclusiveDateTime: Option[ZonedDateTime] =
    numericAndDateRangeRestrictions.minExclusive.map(v =>
      mapJodaDateTimeToZonedDateTime(DateTime.parse(v))
    )
  lazy val maxExclusiveDateTime: Option[ZonedDateTime] =
    numericAndDateRangeRestrictions.maxExclusive.map(v =>
      mapJodaDateTimeToZonedDateTime(DateTime.parse(v))
    )
  lazy val numberParserForFormat: ParseResult[NumberParser] =
    LdmlNumberFormatParser(
      format.flatMap(f => f.groupChar).getOrElse(','),
      format.flatMap(f => f.decimalChar).getOrElse('.')
    ).getParserForFormat(format.flatMap(f => f.pattern).get)

  val datatypeParser: Map[String, String => Either[
    ErrorWithoutContext,
    Any
  ]] =
    Map(
      s"${rdfSyntaxNs}XMLLiteral" -> trimValue,
      s"${rdfSyntaxNs}HTML" -> trimValue,
      "http://www.w3.org/ns/csvw#JSON" -> trimValue,
      s"${xmlSchema}anyAtomicType" -> allValueValid,
      s"${xmlSchema}anyURI" -> trimValue,
      s"${xmlSchema}base64Binary" -> trimValue,
      s"${xmlSchema}hexBinary" -> trimValue,
      s"${xmlSchema}QName" -> trimValue,
      s"${xmlSchema}string" -> allValueValid,
      s"${xmlSchema}normalizedString" -> trimValue,
      s"${xmlSchema}token" -> trimValue,
      s"${xmlSchema}language" -> trimValue,
      s"${xmlSchema}Name" -> trimValue,
      s"${xmlSchema}NMTOKEN" -> trimValue,
      s"${xmlSchema}boolean" -> processBooleanDatatype,
      s"${xmlSchema}decimal" -> processDecimalDatatype,
      s"${xmlSchema}integer" -> processIntegerDatatype,
      s"${xmlSchema}long" -> processLongDatatype,
      s"${xmlSchema}int" -> processIntDatatype,
      s"${xmlSchema}short" -> processShortDatatype,
      s"${xmlSchema}byte" -> processByteDatatype,
      s"${xmlSchema}nonNegativeInteger" -> processNonNegativeInteger,
      s"${xmlSchema}positiveInteger" -> processPositiveInteger,
      s"${xmlSchema}unsignedLong" -> processUnsignedLong,
      s"${xmlSchema}unsignedInt" -> processUnsignedInt,
      s"${xmlSchema}unsignedShort" -> processUnsignedShort,
      s"${xmlSchema}unsignedByte" -> processUnsignedByte,
      s"${xmlSchema}nonPositiveInteger" -> processNonPositiveInteger,
      s"${xmlSchema}negativeInteger" -> processNegativeInteger,
      s"${xmlSchema}double" -> processDoubleDatatype,
      s"${xmlSchema}float" -> processFloatDatatype,
      // Date Time related datatype
      s"${xmlSchema}date" -> processDateDatatype,
      s"${xmlSchema}dateTime" -> processDateTimeDatatype,
      s"${xmlSchema}dateTimeStamp" -> processDateTimeStamp,
      s"${xmlSchema}gDay" -> processGDay,
      s"${xmlSchema}gMonth" -> processGMonth,
      s"${xmlSchema}gMonthDay" -> processGMonthDay,
      s"${xmlSchema}gYear" -> processGYear,
      s"${xmlSchema}gYearMonth" -> processGYearMonth,
      s"${xmlSchema}time" -> processTime,
      s"${xmlSchema}duration" -> processDuration,
      s"${xmlSchema}dayTimeDuration" -> processDayTimeDuration,
      s"${xmlSchema}yearMonthDuration" -> processYearMonthDuration
    )

  val noAdditionalValidation: Function[String, Boolean] = (_: String) => true

  private val datatypeFormatValidation: Map[String, Function[String, Boolean]] =
    Map(
      s"${rdfSyntaxNs}XMLLiteral" -> regexpValidation,
      s"${rdfSyntaxNs}HTML" -> regexpValidation,
      "http://www.w3.org/ns/csvw#JSON" -> regexpValidation,
      s"${xmlSchema}anyAtomicType" -> regexpValidation,
      s"${xmlSchema}anyURI" -> regexpValidation,
      s"${xmlSchema}base64Binary" -> regexpValidation,
      s"${xmlSchema}boolean" -> noAdditionalValidation,
      s"${xmlSchema}date" -> noAdditionalValidation,
      s"${xmlSchema}dateTime" -> noAdditionalValidation,
      s"${xmlSchema}dateTimeStamp" -> noAdditionalValidation,
      s"${xmlSchema}decimal" -> noAdditionalValidation,
      s"${xmlSchema}integer" -> noAdditionalValidation,
      s"${xmlSchema}long" -> noAdditionalValidation,
      s"${xmlSchema}int" -> noAdditionalValidation,
      s"${xmlSchema}short" -> noAdditionalValidation,
      s"${xmlSchema}byte" -> noAdditionalValidation,
      s"${xmlSchema}nonNegativeInteger" -> noAdditionalValidation,
      s"${xmlSchema}positiveInteger" -> noAdditionalValidation,
      s"${xmlSchema}unsignedLong" -> noAdditionalValidation,
      s"${xmlSchema}unsignedInt" -> noAdditionalValidation,
      s"${xmlSchema}unsignedShort" -> noAdditionalValidation,
      s"${xmlSchema}unsignedByte" -> noAdditionalValidation,
      s"${xmlSchema}nonPositiveInteger" -> noAdditionalValidation,
      s"${xmlSchema}negativeInteger" -> noAdditionalValidation,
      s"${xmlSchema}double" -> noAdditionalValidation,
      s"${xmlSchema}duration" -> regexpValidation,
      s"${xmlSchema}dayTimeDuration" -> regexpValidation,
      s"${xmlSchema}yearMonthDuration" -> regexpValidation,
      s"${xmlSchema}float" -> noAdditionalValidation,
      s"${xmlSchema}gDay" -> noAdditionalValidation,
      s"${xmlSchema}gMonth" -> noAdditionalValidation,
      s"${xmlSchema}gMonthDay" -> noAdditionalValidation,
      s"${xmlSchema}gYear" -> noAdditionalValidation,
      s"${xmlSchema}gYearMonth" -> noAdditionalValidation,
      s"${xmlSchema}hexBinary" -> regexpValidation,
      s"${xmlSchema}QName" -> regexpValidation,
      s"${xmlSchema}string" -> regexpValidation,
      s"${xmlSchema}normalizedString" -> regexpValidation,
      s"${xmlSchema}token" -> regexpValidation,
      s"${xmlSchema}language" -> regexpValidation,
      s"${xmlSchema}Name" -> regexpValidation,
      s"${xmlSchema}NMTOKEN" -> regexpValidation,
      s"${xmlSchema}time" -> noAdditionalValidation
    )
  private val logger = Logger(this.getClass.getName)

  def regexpValidation(value: String): Boolean = {
    val regExPattern = format.flatMap(f => f.pattern).get
    val regEx = regExPattern.r
    regEx.pattern.matcher(value).matches()
  }

  def trimValue(
      value: String
  ): Either[ErrorWithoutContext, String] = Right(value.strip())

  def allValueValid(
      value: String
  ): Either[ErrorWithoutContext, String] = {
    Right(value)
  }

  def processBooleanDatatype(
      value: String
  ): Either[ErrorWithoutContext, Boolean] = {
    format.flatMap(f => f.pattern) match {
      case Some(pattern) =>
        val patternValues = pattern.split("""\|""")
        if (patternValues(0) == value) {
          Right(true)
        } else if (patternValues(1) == value) {
          Right(false)
        } else
          Left(
            ErrorWithoutContext(
              "invalid_boolean",
              "Not in list of expected values"
            )
          )
      case None =>
        if (Array[String]("true", "1").contains(value)) {
          Right(true)
        } else if (Array[String]("false", "0").contains(value)) {
          Right(false)
        } else
          Left(
            ErrorWithoutContext(
              "invalid_boolean",
              "Not in default expected values (true/false/1/0)"
            )
          )
    }
  }

  def processDecimalDatatype(
      value: String
  ): Either[ErrorWithoutContext, BigDecimal] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        Column.validDecimalDatatypeRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(BigDecimal(newValue))
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_decimal", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_decimal",
            "Does not match expected decimal format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Right(parsedValue) => Right(parsedValue)
        case Left(warning) =>
          logger.debug(warning)
          Left(ErrorWithoutContext("invalid_decimal", warning.message))
      }
    }
  }

  def processDoubleDatatype(
      value: String
  ): Either[ErrorWithoutContext, Double] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        validDoubleDatatypeRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(replaceInfWithInfinity(newValue).toDouble)
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_double", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_double",
            "Does not match expected Double format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_double", w.message))
        case Right(parsedValue) => Right(parsedValue.doubleValue)
      }
    }
  }

  def processFloatDatatype(
      value: String
  ): Either[ErrorWithoutContext, Float] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (Column.validFloatDatatypeRegex.pattern.matcher(newValue).matches()) {
        Right(replaceInfWithInfinity(newValue).toFloat)
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_float",
            "Does not match expected Float format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_float", w.message))
        case Right(parsedValue) => Right(parsedValue.floatValue)
      }
    }
  }

  /**
    * Scala Double does not recognise INF as infinity
    */
  def replaceInfWithInfinity(value: String): String =
    value.replace("INF", "Infinity")

  def processLongDatatype(
      value: String
  ): Either[ErrorWithoutContext, Long] = {

    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        Column.validLongDatatypeRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(newValue.toLong)
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_long", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_long",
            "Does not match expected long format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_long", w.message))
        case Right(parsedValue) =>
          try {
            Right(parsedValue.longValue)
          } catch {
            case _: Throwable =>
              Left(
                ErrorWithoutContext(
                  "invalid_long",
                  s"Outside long range ${Long.MinValue} - ${Long.MaxValue} (Inclusive)"
                )
              )
          }
      }
    }
  }

  def processIntDatatype(
      value: String
  ): Either[ErrorWithoutContext, Int] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        Column.validIntegerRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(newValue.toInt)
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_int", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_int",
            "Does not match expected int format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_int", w.message))
        case Right(parsedNumber) =>
          val parsedValue = parsedNumber.longValue
          if (parsedValue > Int.MaxValue || parsedValue < Int.MinValue)
            Left(
              ErrorWithoutContext(
                "invalid_int",
                s"Outside Int Range ${Int.MinValue} - ${Int.MaxValue} (inclusive)"
              )
            )
          else Right(parsedValue.intValue)
      }
    }
  }

  def processShortDatatype(
      value: String
  ): Either[ErrorWithoutContext, Short] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        Column.validIntegerRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(newValue.toShort)
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_short", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_short",
            "Does not match expected short format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_short", w.message))
        case Right(parsedNumber) =>
          val parsedValue = parsedNumber.longValue
          if (parsedValue > Short.MaxValue || parsedValue < Short.MinValue)
            Left(
              ErrorWithoutContext(
                "invalid_short",
                s"Outside Short Range ${Short.MinValue} - ${Short.MaxValue} (inclusive)"
              )
            )
          else Right(parsedValue.shortValue())
      }
    }
  }

  def processByteDatatype(
      value: String
  ): Either[ErrorWithoutContext, Byte] = {
    if (patternIsEmpty()) {
      if (
        Column.validIntegerRegex.pattern
          .matcher(value)
          .matches()
      ) {
        try {
          Right(value.toByte)
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_byte", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_byte",
            "Does not match expected byte format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Left(w) =>
          logger.debug(w)
          Left(ErrorWithoutContext("invalid_byte", w.message))
        case Right(parsedNumber) =>
          val parsedValue = parsedNumber.byteValue
          if (parsedValue > Byte.MaxValue || parsedValue < Byte.MinValue)
            Left(
              ErrorWithoutContext(
                "invalid_byte",
                s"Outside Byte Range ${Byte.MinValue} - ${Byte.MaxValue} (inclusive)"
              )
            )
          else Right(parsedValue.byteValue())
      }
    }
  }

  def processPositiveInteger(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    val result = processIntegerDatatype(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_positiveInteger", w.content))
      case Right(parsedValue) =>
        if (parsedValue <= 0) {
          Left(
            ErrorWithoutContext(
              "invalid_positiveInteger",
              "Value less than or equal to 0"
            )
          )
        } else Right(parsedValue)
    }
  }

  def processUnsignedLong(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    val result = processNonNegativeInteger(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_unsignedLong", w.content))
      case Right(parsedValue) =>
        if (parsedValue > unsignedLongMaxValue) {
          Left(
            ErrorWithoutContext(
              "invalid_unsignedLong",
              "Value greater than 18446744073709551615"
            )
          )
        } else Right(parsedValue)
    }
  }

  def processNonNegativeInteger(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    val result = processIntegerDatatype(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_nonNegativeInteger", w.content))
      case Right(parsedValue) =>
        if (parsedValue < 0) {
          Left(
            ErrorWithoutContext(
              "invalid_nonNegativeInteger",
              "Value less than 0"
            )
          )
        } else Right(parsedValue)
    }
  }

  def processIntegerDatatype(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    if (patternIsEmpty()) {
      val newValue = standardisedValue(value)
      if (
        Column.validIntegerRegex.pattern
          .matcher(newValue)
          .matches()
      ) {
        try {
          Right(new BigInteger(newValue))
        } catch {
          case e: Throwable =>
            logger.debug(e)
            Left(ErrorWithoutContext("invalid_integer", e.getMessage))
        }
      } else {
        Left(
          ErrorWithoutContext(
            "invalid_integer",
            "Does not match expected integer format"
          )
        )
      }
    } else {
      parseNumberAgainstFormat(value) match {
        case Right(parsedValue) => convertToBigIntegerValue(parsedValue)
        case Left(warning) =>
          logger.debug(warning)
          Left(ErrorWithoutContext("invalid_integer", warning.message))
      }
    }
  }

  private def patternIsEmpty(): Boolean =
    format
      .flatMap(_.pattern)
      .isEmpty

  def parseNumberAgainstFormat(
      value: String
  ): ParseResult[BigDecimal] =
    numberParserForFormat.flatMap(_.parse(value))

  /**
    * In CSV-W, grouping characters can be used to group numbers in a decimal but this grouping
    * char won't be recognised by the regular expression we use to validate decimal at a later stage.
    * This function removes grouping character if any and replaces any custom decimal character with the default
    * decimal character.
    */
  def standardisedValue(value: String): String = {
    val lastChar = value.takeRight(1)
    var newValue =
      if (lastChar == "%" || lastChar == "‰")
        value.substring(0, value.length - 1)
      else value
    newValue = groupChar match {
      case Some(groupChar) =>
        s"(?<=[0-9])$groupChar(?=[0-9])".r.replaceAllIn(newValue, "")
      case None => newValue
    }

    newValue = decimalChar match {
      case Some(decimalChar) =>
        s"(?<=[0-9])$decimalChar(?=[0-9])".r.replaceAllIn(newValue, ".")
      case None => newValue
    }

    newValue
  }

  def groupChar: Option[Char] = {
    format.flatMap(_.groupChar)
  }

  def decimalChar: Option[Char] = {
    format.flatMap(_.decimalChar)
  }

  private def convertToBigIntegerValue(
      parsedValue: BigDecimal
  ): Either[ErrorWithoutContext, BigInteger] = {
    try {
      Right(parsedValue.toBigInt.bigInteger)
    } catch {
      case e: Throwable =>
        Left(ErrorWithoutContext("invalid_integer", e.getMessage))
    }
  }

  def processUnsignedInt(
      value: String
  ): Either[ErrorWithoutContext, Long] = {
    val result = processNonNegativeInteger(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_unsignedInt", w.content))
      case Right(parsedValue) =>
        if (parsedValue > 4294967295L) {
          Left(
            ErrorWithoutContext(
              "invalid_unsignedInt",
              "Value greater than 4294967295"
            )
          )
        } else Right(parsedValue.longValue())
    }
  }

  def processUnsignedShort(
      value: String
  ): Either[ErrorWithoutContext, Long] = {
    val result = processNonNegativeInteger(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_unsignedShort", w.content))
      case Right(parsedValue) =>
        if (parsedValue > 65535) {
          Left(
            ErrorWithoutContext(
              "invalid_unsignedShort",
              "Value greater than 65535"
            )
          )
        } else Right(parsedValue.intValue())
    }
  }

  def processUnsignedByte(
      value: String
  ): Either[ErrorWithoutContext, Short] = {
    val result = processNonNegativeInteger(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_unsignedByte", w.content))
      case Right(parsedValue) =>
        if (parsedValue > 255) {
          Left(ErrorWithoutContext("invalid_unsignedByte", "Greater than 255"))
        } else Right(parsedValue.shortValue())
    }
  }

  def processNonPositiveInteger(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    val result = processIntegerDatatype(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_nonPositiveInteger", w.content))
      case Right(parsedValue) =>
        if (parsedValue > 0) {
          Left(
            ErrorWithoutContext(
              "invalid_nonPositiveInteger",
              "Parsed value greater than 0"
            )
          )
        } else Right(parsedValue)
    }
  }

  def processNegativeInteger(
      value: String
  ): Either[ErrorWithoutContext, BigInteger] = {
    val result = processIntegerDatatype(value)
    result match {
      case Left(w) =>
        Left(ErrorWithoutContext("invalid_negativeInteger", w.content))
      case Right(parsedValue) =>
        if (parsedValue >= 0) {
          Left(
            ErrorWithoutContext(
              "invalid_negativeInteger",
              "Value greater than 0"
            )
          )
        } else Right(parsedValue)
    }
  }

  def containsUnquotedChar(value: String, char: Char): Boolean = {
    var insideQuotes = false

    for (c <- value) {
      if (c == '\'') {
        insideQuotes = !insideQuotes
      } else if (c == char && !insideQuotes) {
        return true
      }
    }
    false
  }

  def stripUnquotedPlusMinus(
      value: String,
      removeUnquotedPluses: Boolean = true,
      removeUnquotedMinuses: Boolean = true
  ): String = {
    var insideQuotes = false
    val filteredChars = new StringBuilder()
    for (char <- value) {
      if (char == '\'') {
        insideQuotes = !insideQuotes
      } else if (char == '+' && !insideQuotes) {
        if (!removeUnquotedPluses) {
          filteredChars.append(char)
        }
      } else if (char == '-' && !insideQuotes) {
        if (!removeUnquotedMinuses) {
          filteredChars.append(char)
        }
      } else {
        filteredChars.append(char)
      }
    }
    filteredChars.toString()
  }

  def processDateDatatype(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}date",
      "invalid_date",
      value
    )
  }

  def dateTimeParser(
      datatype: String,
      warning: String,
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    val dateFormatObject = DateFormat(format.flatMap(f => f.pattern), datatype)
    dateFormatObject.parse(value) match {
      case Right(value) => Right(value)
      case Left(error)  => Left(ErrorWithoutContext(warning, error))
    }
  }

  def processDateTimeDatatype(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}dateTime",
      "invalid_datetime",
      value
    )
  }

  def processDateTimeStamp(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}dateTimeStamp",
      "invalid_dateTimeStamp",
      value
    )
  }

  def processGDay(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}gDay",
      "invalid_gDay",
      value
    )
  }

  def processGMonth(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}gMonth",
      "invalid_gMonth",
      value
    )
  }

  def processGMonthDay(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}gMonthDay",
      "invalid_gMonthDat",
      value
    )
  }

  def processGYear(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}gYear",
      "invalid_gYear",
      value
    )
  }

  def processGYearMonth(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}gYearMonth",
      "invalid_gYearMonth",
      value
    )
  }

  def processTime(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}time",
      "invalid_time",
      value
    )
  }

  def processDuration(
      value: String
  ): Either[ErrorWithoutContext, String] = {
    if (!Column.validDurationRegex.pattern.matcher(value).matches()) {
      Left(
        ErrorWithoutContext(
          "invalid_duration",
          "Does not match expected duration format"
        )
      )
    } else Right(value)
  }

  def processDayTimeDuration(
      value: String
  ): Either[ErrorWithoutContext, String] = {
    if (!Column.validDayTimeDurationRegex.pattern.matcher(value).matches()) {
      Left(
        ErrorWithoutContext(
          "invalid_dayTimeDuration",
          "Does not match expected dayTimeDuration format"
        )
      )
    } else Right(value)
  }

  def processYearMonthDuration(
      value: String
  ): Either[ErrorWithoutContext, String] = {
    if (!Column.validYearMonthDurationRegex.pattern.matcher(value).matches()) {
      Left(
        ErrorWithoutContext(
          "invalid_yearMonthDuration",
          "Does not match expected yearMonthDuration format"
        )
      )
    } else Right(value)
  }

  def validate(
      value: String
  ): (Array[ErrorWithoutContext], List[Any]) = {
    val errors = ArrayBuffer.empty[ErrorWithoutContext]
    if (nullParam.contains(value)) {
      // Since the cell value is among the null values specified for this CSV-W, it can be considered as the default null value which is ""
      val errorWithoutContext = getErrorIfRequiredValueAndValueEmpty("")
      if (errorWithoutContext.isDefined) {
        errors.addOne(errorWithoutContext.get)
      }
      (errors.toArray, List.empty)
    } else {
      val valuesArrayToReturn = ArrayBuffer.empty[Any]
      val values = separator match {
        case Some(separator) => value.split(separator)
        case None            => Array[String](value)
      }
      val parserForDataType = datatypeParser(baseDataType)
      for (v <- values) {
        parserForDataType(v) match {
          case Left(errorMessageContent) =>
            errors.addOne(
              ErrorWithoutContext(
                errorMessageContent.`type`,
                s"'$v' - ${errorMessageContent.content} (${format.flatMap(_.pattern).getOrElse("no format provided")})"
              )
            )
            valuesArrayToReturn.addOne(s"invalid - $v")
          case Right(s) =>
            errors.addAll(validateLength(s.toString))
            errors.addAll(validateValue(s))
            getErrorIfRequiredValueAndValueEmpty(s.toString) match {
              case Some(e) => errors.addOne(e)
              case None    =>
            }
            validateFormat(s.toString) match {
              case Some(e) => errors.addOne(e)
              case None    =>
            }

            if (errors.isEmpty) {
              valuesArrayToReturn.addOne(s)
            }
        }
      }
      (errors.toArray, valuesArrayToReturn.toList)
    }
  }

  def getErrorIfRequiredValueAndValueEmpty(
      value: String
  ): Option[ErrorWithoutContext] = {
    if (required && value.isEmpty) {
      Some(ErrorWithoutContext("Required", value))
    } else None
  }

  def validateFormat(value: String): Option[ErrorWithoutContext] = {
    format match {
      case Some(f) =>
        val formatValidator = datatypeFormatValidation(baseDataType)
        if (formatValidator(value)) None
        else {
          Some(
            ErrorWithoutContext(
              "format",
              s"Value in csv does not match the format specified in metadata, Value: $value Format: ${f.pattern.get}"
            )
          )
        }
      case None => None
    }
  }

  def validateLength(
      value: String
  ): Array[ErrorWithoutContext] = {
    if (
      lengthRestrictions.length.isEmpty && lengthRestrictions.minLength.isEmpty && lengthRestrictions.maxLength.isEmpty
    ) {
      Array.ofDim(0)
    } else {
      val errors = ArrayBuffer.empty[ErrorWithoutContext]
      var lengthOfValue = value.length
      if (baseDataType == s"${xmlSchema}base64Binary") {
        lengthOfValue = value.replaceAll("==?$", "").length * 3 / 4
      } else if (baseDataType == s"${xmlSchema}hexBinary") {
        lengthOfValue = value.length / 2
      }
      if (
        lengthRestrictions.minLength.isDefined && lengthOfValue < lengthRestrictions.minLength.get
      ) {
        errors.addOne(
          ErrorWithoutContext(
            "minLength",
            s"value '$value' length less than minLength specified - $lengthRestrictions.minLength"
          )
        )
      }
      if (
        lengthRestrictions.maxLength.isDefined && lengthOfValue > lengthRestrictions.maxLength.get
      ) {
        errors.addOne(
          ErrorWithoutContext(
            "maxLength",
            s"value '$value' length greater than maxLength specified - $lengthRestrictions.maxLength"
          )
        )
      }
      if (
        lengthRestrictions.length.isDefined && lengthOfValue != lengthRestrictions.length.get
      ) {
        errors.addOne(
          ErrorWithoutContext(
            "length",
            s"value '$value' length different from length specified - ${lengthRestrictions.length.get}"
          )
        )
      }
      errors.toArray
    }
  }

  def validateValue(value: Any): Array[ErrorWithoutContext] =
    value match {
      case numericValue: Number    => validateNumericValue(numericValue)
      case _: String               => Array[ErrorWithoutContext]()
      case datetime: ZonedDateTime => validateDateTimeValue(datetime)
      case _: Boolean              => Array[ErrorWithoutContext]()
      case _ =>
        throw new IllegalArgumentException(
          s"Have not mapped ${value.getClass} yet."
        )
    }

  private def validateDateTimeValue(
      datetime: ZonedDateTime
  ): Array[ErrorWithoutContext] =
    checkValueRangeConstraints[ZonedDateTime](
      datetime,
      dtValue => minInclusiveDateTime.exists(dtValue.compareTo(_) < 0),
      dtValue => minExclusiveDateTime.exists(dtValue.compareTo(_) <= 0),
      dtValue => maxInclusiveDateTime.exists(v => dtValue.compareTo(v) > 0),
      dtValue => maxExclusiveDateTime.exists(dtValue.compareTo(_) >= 0)
    )

  private def validateNumericValue(
      numericValue: Number
  ): Array[ErrorWithoutContext] = {
    numericValue match {
      case _: java.lang.Long | _: Integer | _: java.lang.Short |
          _: java.lang.Float | _: java.lang.Double | _: java.lang.Byte =>
        checkValueRangeConstraints[Long](
          numericValue.longValue(),
          longValue => minInclusiveNumeric.exists(longValue < _),
          longValue => minExclusiveNumeric.exists(longValue <= _),
          longValue => maxInclusiveNumeric.exists(longValue > _),
          longValue => maxExclusiveNumeric.exists(longValue >= _)
        )
      case bd: BigDecimal =>
        checkValueRangeConstraints[BigDecimal](
          bd,
          bigDecimalValue => minInclusiveNumeric.exists(bigDecimalValue < _),
          bigDecimalValue => minExclusiveNumeric.exists(bigDecimalValue <= _),
          bigDecimalValue => maxInclusiveNumeric.exists(bigDecimalValue > _),
          bigDecimalValue => maxExclusiveNumeric.exists(bigDecimalValue >= _)
        )
      case bi: BigInteger =>
        checkValueRangeConstraints[BigInt](
          BigInt(bi),
          bigIntValue => minInclusiveInt.exists(bigIntValue < _),
          bigIntValue => minExclusiveInt.exists(bigIntValue <= _),
          bigIntValue => maxInclusiveInt.exists(bigIntValue > _),
          bigIntValue => maxExclusiveInt.exists(bigIntValue >= _)
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Unmatched numeric type ${numericValue.getClass}"
        )
    }
  }

  def checkValueRangeConstraints[T](
      value: T,
      lessThanMinInclusive: T => Boolean,
      lessThanEqualToMinExclusive: T => Boolean,
      greaterThanMaxInclusive: T => Boolean,
      greaterThanEqualToMaxExclusive: T => Boolean
  ): Array[ErrorWithoutContext] = {
    val errors = ArrayBuffer.empty[ErrorWithoutContext]
    if (lessThanMinInclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "minInclusive",
          s"value '$value' less than minInclusive value '${numericAndDateRangeRestrictions.minInclusive.get}'"
        )
      )
    }
    if (greaterThanMaxInclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "maxInclusive",
          s"value '$value' greater than maxInclusive value '${numericAndDateRangeRestrictions.maxInclusive.get}'"
        )
      )
    }
    if (lessThanEqualToMinExclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "minExclusive",
          s"value '$value' less than or equal to minExclusive value '${numericAndDateRangeRestrictions.minExclusive.get}'"
        )
      )
    }
    if (greaterThanEqualToMaxExclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "maxExclusive",
          s"value '$value' greater than or equal to maxExclusive value '${numericAndDateRangeRestrictions.maxExclusive.get}'"
        )
      )
    }
    errors.toArray
  }

  def validateHeader(columnName: String): WarningsAndErrors = {
    var errors = Array[ErrorWithCsvContext]()
    var validHeaders = Array[String]()
    for (titleLanguage <- titleValues.keys) {
      if (Column.languagesMatch(titleLanguage, lang)) {
        validHeaders ++= titleValues(titleLanguage)
      }
    }
    if (!validHeaders.contains(columnName)) {
      errors :+= ErrorWithCsvContext(
        "Invalid Header",
        "Schema",
        "1",
        columnOrdinal.toString,
        columnName,
        titleValues.mkString(",")
      )
    }
    models.WarningsAndErrors(Array(), errors)
  }

  private def mapJodaDateTimeToZonedDateTime(jDt: DateTime) = {
    val zoneId: ZoneId = jDt.getZone.toTimeZone.toZoneId
    val localDateTime: LocalDateTime =
      LocalDateTime.of(
        jDt.getYear,
        Month.of(jDt.getMonthOfYear),
        jDt.getDayOfMonth,
        jDt.getHourOfDay,
        jDt.getMinuteOfHour,
        jDt.getSecondOfMinute,
        jDt.getMillisOfSecond * 1000
      )
    ZonedDateTime.of(localDateTime, zoneId)
  }
}
