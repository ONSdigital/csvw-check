package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import com.typesafe.scalalogging.Logger
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, ErrorWithoutContext, MetadataError}
import csvwcheck.helpers.MapHelpers
import csvwcheck.models.Column.{rdfSyntaxNs, unsignedLongMaxValue, validDoubleDatatypeRegex, xmlSchema}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.traits.LoggerExtensions.LogDebugException
import csvwcheck.traits.NumberParser
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import csvwcheck.{PropertyChecker, models}
import org.joda.time.{DateTime, DateTimeZone}

import java.math.BigInteger
import java.time.{LocalDateTime, Month, ZoneId, ZonedDateTime}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.math.BigInt.javaBigInteger2bigInt
import scala.util.matching.Regex

object Column {
  val xmlSchema = "http://www.w3.org/2001/XMLSchema#"
  val datatypeDefaultValue: ObjectNode = JsonNodeFactory.instance
    .objectNode()
    .set("@id", new TextNode(s"${xmlSchema}string"))
  val rdfSyntaxNs = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  private val validDecimalDatatypeRegex =
    "(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)".r

  // https://www.w3.org/TR/xmlschema11-2/#float, https://www.w3.org/TR/xmlschema11-2/#double
  val validDoubleDatatypeRegex, validFloatDatatypeRegex =
    "(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?|(\\+|-)?INF|NaN".r

  private val validIntegerRegex = "[\\-+]?[0-9]+".r

  private val validLongDatatypeRegex = "[\\-+]?[0-9]+".r

  // https://www.w3.org/TR/xmlschema11-2/#duration
  val validDurationRegex: Regex =
    "-?P((([0-9]+Y([0-9]+M)?([0-9]+D)?|([0-9]+M)([0-9]+D)?|([0-9]+D))(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S)))?)|(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S))))".r
  val validDayTimeDurationRegex: Regex =
    "-?P(([0-9]+D(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S)))?)|(T(([0-9]+H)([0-9]+M)?([0-9]+(\\.[0-9]+)?S)?|([0-9]+M)([0-9]+(\\.[0-9]+)?S)?|([0-9]+(\\.[0-9]+)?S))))".r
  val validYearMonthDurationRegex: Regex =
    """-?P([0-9]+Y([0-9]+M)?|([0-9]+M))""".r

  val unsignedLongMaxValue: BigInt = BigInt("18446744073709551615")

  def fromJson(
      columnOrdinal: Int,
      columnDesc: ObjectNode,
      baseUrl: String,
      lang: String,
      inheritedProperties: mutable.Map[String, JsonNode]
  ): ParseResult[(Column, Array[ErrorWithoutContext])] = {
      partitionAndValidateColumnPropertiesByType(
        columnDesc,
        columnOrdinal,
        baseUrl,
        lang,
        inheritedProperties
      )
        .map(parsedColumnProperties => {
          val datatype = getDatatypeOrDefault(inheritedProperties)
          val minLength: Option[Int] =
            if (datatype.path("minLength").isMissingNode) None
            else Some(datatype.get("minLength").asText().toInt)
          val maxLength: Option[Int] =
            if (datatype.path("maxLength").isMissingNode) None
            else Some(datatype.get("maxLength").asText().toInt)
          val length: Option[Int] =
            if (datatype.path("length").isMissingNode) None
            else Some(datatype.get("length").asText().toInt)

          val minInclusive: Option[String] =
            if (datatype.path("minInclusive").isMissingNode) None
            else Some(datatype.get("minInclusive").asText)
          val maxInclusive: Option[String] =
            if (datatype.path("maxInclusive").isMissingNode) None
            else Some(datatype.get("maxInclusive").asText)
          val minExclusive: Option[String] =
            if (datatype.path("minExclusive").isMissingNode) None
            else Some(datatype.get("minExclusive").asText)
          val maxExclusive: Option[String] =
            if (datatype.path("maxExclusive").isMissingNode) None
            else Some(datatype.get("maxExclusive").asText)

          val newLang = getLangOrDefault(inheritedProperties)

          val name = getName(parsedColumnProperties.column, lang)
          val formatNode = datatype.path("format")

          val titles = parsedColumnProperties.column.get("titles")

          val column = new Column(
            columnOrdinal = columnOrdinal,
            name = name,
            id = getId(parsedColumnProperties.column),
            minLength = minLength,
            maxLength = maxLength,
            length = length,
            minInclusive = minInclusive,
            maxInclusive = maxInclusive,
            minExclusive = minExclusive,
            maxExclusive = maxExclusive,
            baseDataType = getBaseDataType(datatype),
            lang = newLang,
            nullParam = getNullParam(inheritedProperties),
            default = getDefault(inheritedProperties),
            required = getRequired(inheritedProperties),
            aboutUrl = getAboutUrl(inheritedProperties),
            propertyUrl = getPropertyUrl(inheritedProperties),
            valueUrl = getValueUrl(inheritedProperties),
            separator = getSeparator(inheritedProperties),
            ordered = getOrdered(inheritedProperties),
            titleValues = getTitleValues(titles),
            suppressOutput = getSuppressOutput(parsedColumnProperties.column),
            virtual = getVirtual(parsedColumnProperties.column),
            format = getMaybeFormatForColumn(formatNode),
            textDirection = getTextDirection(inheritedProperties),
            annotations = parsedColumnProperties.annotation
          )

          (column, parsedColumnProperties.warnings)
        })
  }

  def getOrdered(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Boolean = {
    val inheritedPropertiesNode = inheritedProperties.get("ordered")
    inheritedPropertiesNode match {
      case Some(value) => value.asBoolean()
      case _           => false
    }
  }

  def getTextDirection(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): String = {
    val textDirectionNode = inheritedProperties.get("textDirection")
    textDirectionNode match {
      case Some(value) => value.asText()
      case _           => "inherit"
    }
  }

  def getSuppressOutput(
      columnProperties: mutable.Map[String, JsonNode]
  ): Boolean = {
    val suppressOutputNode = columnProperties.get("suppressOutput")
    suppressOutputNode match {
      case Some(value) => value.asBoolean()
      case _           => false
    }
  }

  def getVirtual(columnProperties: mutable.Map[String, JsonNode]): Boolean = {
    val virtualNode = columnProperties.get("virtual")
    virtualNode match {
      case Some(value) => value.asBoolean()
      case _           => false
    }
  }

  def getRequired(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Boolean = {
    inheritedProperties.get("required") match {
      case Some(value) => value.asBoolean()
      case _           => false
    }
  }

  def getDefault(inheritedProperties: mutable.Map[String, JsonNode]): String = {
    inheritedProperties.get("default") match {
      case Some(value) => value.asInstanceOf[TextNode].asText()
      case _           => ""
    }
  }

  def getId(columnProperties: mutable.Map[String, JsonNode]): Option[String] = {
    val idNode = columnProperties.get("@id")
    if (idNode.isDefined) Some(idNode.get.asText()) else None
  }

  def getName(
      columnProperties: mutable.Map[String, JsonNode],
      lang: String
  ): Option[String] = {
    val name = columnProperties.get("name")
    val titles = columnProperties.get("titles")

    if (name.isDefined && !name.get.isNull) {
      Some(name.get.asInstanceOf[TextNode].asText())
    } else if (titles.isDefined && titles.get.path(lang).isMissingNode) {
      val langArray = Array.from(
        titles.get.path(lang).elements().asScala
      )
      if (langArray.nonEmpty) {
        Some(langArray(0).asText())
      } else None
    } else None // Not sure what to return here. Hope it does not reach here
  }

  def getNullParam(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Array[String] = {
    inheritedProperties.get("null") match {
      case Some(value) =>
        value match {
          case a: ArrayNode =>
            var nullParamsToReturn = Array[String]()
            val nullParams = Array.from(a.elements.asScala)
            for (np <- nullParams)
              nullParamsToReturn :+= np.asText()
            nullParamsToReturn
          case s: TextNode => Array[String](s.asText())
          case _ =>
            throw MetadataError("unexpected value for null property")
        }
      case None => Array[String]("")
    }
  }

  def getAboutUrl(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Option[String] = {
    val aboutUrlNode = inheritedProperties.get("aboutUrl")
    aboutUrlNode match {
      case Some(value) => Some(value.asText())
      case _           => None
    }
  }

  def getPropertyUrl(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Option[String] = {
    val propertyUrlNode = inheritedProperties.get("propertyUrl")
    propertyUrlNode match {
      case Some(value) => Some(value.asText())
      case _           => None
    }
  }

  def getValueUrl(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Option[String] = {
    val valueUrlNode = inheritedProperties.get("valueUrl")
    valueUrlNode match {
      case Some(value) => Some(value.asText())
      case _           => None
    }
  }

  def getSeparator(
      inheritedProperties: mutable.Map[String, JsonNode]
  ): Option[String] = {
    val separatorNode = inheritedProperties.get("separator")
    separatorNode match {
      case Some(value) => Some(value.asText())
      case _           => None
    }
  }

  def getTitleValues(
      titles: Option[JsonNode]
  ): mutable.Map[String, Array[String]] = {
    val langToTitles = mutable.Map[String, Array[String]]()
    titles match {
      case Some(titles) =>
        for ((l, v) <- titles.asInstanceOf[ObjectNode].getKeysAndValues) {
          langToTitles(l) = Array.from(v.elements().asScala.map(_.asText()))
        }
        langToTitles
      case _ => langToTitles
    }
  }

  def getBaseDataType(datatype: JsonNode): String = {
    val datatypeBaseField = datatype.path("base")
    if (datatypeBaseField.isMissingNode) {
      datatype.get("@id").asText()
    } else {
      datatypeBaseField.asText()
    }
  }

  case class ParsedColumnProperties(
      inherited: Map[String, JsonNode] = Map(),
      annotation: Map[String, JsonNode] = Map(),
      column: Map[String, JsonNode] = Map(),
      warnings: Array[ErrorWithoutContext] = Array()
  ) {
    def withInheritedProperty(keyValuePair: (String, JsonNode)) =
      ParsedColumnProperties(
        inherited = inherited + keyValuePair,
        annotation = annotation,
        column = column,
        warnings = warnings
      )

    def withAnnotationProperty(keyValuePair: (String, JsonNode)) =
      ParsedColumnProperties(
        inherited = inherited,
        annotation = annotation + keyValuePair,
        column = column,
        warnings = warnings
      )

    def withColumnProperty(keyValuePair: (String, JsonNode)) =
      ParsedColumnProperties(
        inherited = inherited,
        annotation = annotation,
        column = column + keyValuePair,
        warnings = warnings
      )

    def withWarning(warning: ErrorWithoutContext) =
      ParsedColumnProperties(
        inherited = inherited,
        annotation = annotation,
        column = column,
        warnings = warnings :+ warning
      )

    def withWarnings(warnings: Array[ErrorWithoutContext]) =
      ParsedColumnProperties(
        inherited = inherited,
        annotation = annotation,
        column = column,
        warnings = warnings ++ warnings
      )
  }

  def partitionAndValidateColumnPropertiesByType(
      columnDesc: ObjectNode,
      columnOrdinal: Int,
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode]
  ): ParsedResult[ParsedColumnProperties] = {
    val initialAccumulator: ParsedResult[ParsedColumnProperties] = Right(
      ParsedColumnProperties(inherited = inheritedProperties)
    )

    columnDesc.getKeysAndValues
      .map({
        case ("@type", textNode: TextNode) if textNode.asText != "Column" =>
          Left(MetadataError(s"columns[$columnOrdinal].@type, @type of column is not 'Column'"))
        case (propertyName, value) =>
          PropertyChecker
            .parseJsonProperty(propertyName, value, baseUrl, lang)
            .map({ case (v, ws, pt) => (propertyName, v, ws, pt) })
      })
      .foldLeft(initialAccumulator)({
        case (err @ Left(_), _) => err
        case (_, Left(newErr))  => Left(newErr)
        case (
              Right(parsedProperties),
              Right((propertyName, parsedValue, stringWarnings, propertyType))
            ) =>
          val partitionedProperties = (propertyType match {
            case PropertyType.Inherited =>
              parsedProperties
                .withInheritedProperty(propertyName -> parsedValue)
            case PropertyType.Annotation =>
              parsedProperties
                .withAnnotationProperty(propertyName -> parsedValue)
            case PropertyType.Common | PropertyType.Column =>
              parsedProperties.withColumnProperty(propertyName -> parsedValue)
            case _ =>
              parsedProperties.withWarning(
                ErrorWithoutContext(
                  s"invalid_property",
                  s"column: $propertyName"
                )
              )
          }).withWarnings(
              stringWarnings.map(
                ErrorWithoutContext(
                  _,
                  s"$propertyName: ${parsedValue.toPrettyString}"
                )
              )
            )
          Right(partitionedProperties)
      })
  }

  private def getMaybeFormatForColumn(formatNode: JsonNode): Option[Format] = {
    if (formatNode.isMissingNode || formatNode.isNull) {
      None
    } else {
      formatNode match {
        case s: TextNode => Some(Format(Some(s.asText()), None, None))
        case _ =>
          val formatObjectNode = formatNode.asInstanceOf[ObjectNode]

          def getMaybeValueFromNode(
              node: ObjectNode,
              propertyName: String
          ): Option[String] = {
            if (node.isMissingNode) None
            else node.getMaybeNode(propertyName).map(_.asText())
          }

          val pattern = getMaybeValueFromNode(formatObjectNode, "pattern")
          val decimalChar =
            getMaybeValueFromNode(formatObjectNode, "decimalChar")
              .map(d => d(0))
          val groupChar = getMaybeValueFromNode(formatObjectNode, "groupChar")
            .map(d => d(0))

          Some(Format(pattern, decimalChar, groupChar))
      }
    }
  }

  private def getLangOrDefault(
      inheritedPropertiesCopy: mutable.Map[String, JsonNode]
  ): String = {
    inheritedPropertiesCopy.get("lang") match {
      case Some(lang) => lang.asText()
      case _          => "und"
    }
  }

  private def getDatatypeOrDefault(
      inheritedPropertiesCopy: mutable.Map[String, JsonNode]
  ): JsonNode = {
    inheritedPropertiesCopy.get("datatype") match {
      case Some(datatype) => datatype
      case _              => datatypeDefaultValue
    }
  }

  def languagesMatch(l1: String, l2: String): Boolean = {
    val languagesMatchOrEitherIsUndefined =
      l1 == l2 || l1 == "und" || l2 == "und"
    val oneLanguageIsSubClassOfAnother =
      l1.startsWith(s"$l2-") || l2.startsWith(s"$l1-")

    languagesMatchOrEitherIsUndefined || oneLanguageIsSubClassOfAnother
  }

}

case class Column private (
    columnOrdinal: Int,
    name: Option[String],
    id: Option[String],
    aboutUrl: Option[String],
    minLength: Option[Int],
    maxLength: Option[Int],
    length: Option[Int],
    minInclusive: Option[String],
    maxInclusive: Option[String],
    minExclusive: Option[String],
    maxExclusive: Option[String],
    baseDataType: String,
    default: String,
    lang: String,
    nullParam: Array[String],
    ordered: Boolean,
    propertyUrl: Option[String],
    required: Boolean,
    separator: Option[String],
    suppressOutput: Boolean,
    textDirection: String,
    titleValues: mutable.Map[String, Array[String]],
    valueUrl: Option[String],
    virtual: Boolean,
    format: Option[Format],
    annotations: mutable.Map[String, JsonNode]
) {
  lazy val minInclusiveNumeric: Option[BigDecimal] =
    minInclusive.map(BigDecimal(_))
  lazy val maxInclusiveNumeric: Option[BigDecimal] =
    maxInclusive.map(BigDecimal(_))
  lazy val minExclusiveNumeric: Option[BigDecimal] =
    minExclusive.map(BigDecimal(_))
  lazy val maxExclusiveNumeric: Option[BigDecimal] =
    maxExclusive.map(BigDecimal(_))
  lazy val minInclusiveInt: Option[BigInt] = minInclusiveNumeric.map(_.toBigInt)
  lazy val maxInclusiveInt: Option[BigInt] = maxInclusiveNumeric.map(_.toBigInt)
  lazy val minExclusiveInt: Option[BigInt] = minExclusiveNumeric.map(_.toBigInt)
  lazy val maxExclusiveInt: Option[BigInt] = maxExclusiveNumeric.map(_.toBigInt)
  lazy val minInclusiveDateTime: Option[ZonedDateTime] =
    minInclusive.map(v => mapJodaDateTimeToZonedDateTime(DateTime.parse(v)))

  DateTimeZone.setDefault(DateTimeZone.UTC)
  lazy val maxInclusiveDateTime: Option[ZonedDateTime] =
    maxInclusive.map(v => mapJodaDateTimeToZonedDateTime(DateTime.parse(v)))
  lazy val minExclusiveDateTime: Option[ZonedDateTime] =
    minExclusive.map(v => mapJodaDateTimeToZonedDateTime(DateTime.parse(v)))
  lazy val maxExclusiveDateTime: Option[ZonedDateTime] =
    maxExclusive.map(v => mapJodaDateTimeToZonedDateTime(DateTime.parse(v)))
  lazy val numberParserForFormat: Either[String, NumberParser] =
    try {
      Right(
        LdmlNumberFormatParser(
          format.flatMap(f => f.groupChar).getOrElse(','),
          format.flatMap(f => f.decimalChar).getOrElse('.')
        ).getParserForFormat(format.flatMap(f => f.pattern).get)
      )
    } catch {
      case e: Throwable =>
        logger.debug(e)
        Left(e.getMessage)
    }
  val datatypeParser: mutable.Map[String, String => Either[
    ErrorWithoutContext,
    Any
  ]] =
    mutable.Map(
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

  private val datatypeFormatValidation
      : mutable.Map[String, Function[String, Boolean]] = mutable.Map(
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
          Left(ErrorWithoutContext("invalid_decimal", warning))
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
        case Left(w)            => Left(ErrorWithoutContext("invalid_double", w))
        case Right(parsedValue) => Right(parsedValue.doubleValue)
      }
    }
  }

  /**
    * Scala Double does not recognise INF as infinity
    */
  def replaceInfWithInfinity(value: String): String =
    value.replace("INF", "Infinity")

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
        case Left(w)            => Left(ErrorWithoutContext("invalid_float", w))
        case Right(parsedValue) => Right(parsedValue.floatValue)
      }
    }
  }

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
        case Left(w) => Left(ErrorWithoutContext("invalid_long", w))
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
        case Left(w) => Left(ErrorWithoutContext("invalid_int", w))
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
        case Left(w) => Left(ErrorWithoutContext("invalid_short", w))
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

  private def patternIsEmpty(): Boolean =
    format
      .flatMap(_.pattern)
      .isEmpty

  def parseNumberAgainstFormat(
      value: String
  ): Either[String, BigDecimal] =
    numberParserForFormat match {
      case Right(numericParser) => numericParser.parse(value)
      case Left(err)            => Left(err)
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
        case Left(w) => Left(ErrorWithoutContext("invalid_byte", w))
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
          Left(ErrorWithoutContext("invalid_integer", warning))
      }
    }
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

  def processDateTimeDatatype(
      value: String
  ): Either[ErrorWithoutContext, ZonedDateTime] = {
    dateTimeParser(
      s"${xmlSchema}dateTime",
      "invalid_datetime",
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
    if (length.isEmpty && minLength.isEmpty && maxLength.isEmpty) {
      Array.ofDim(0)
    } else {
      val errors = ArrayBuffer.empty[ErrorWithoutContext]
      var lengthOfValue = value.length
      if (baseDataType == s"${xmlSchema}base64Binary") {
        lengthOfValue = value.replaceAll("==?$", "").length * 3 / 4
      } else if (baseDataType == s"${xmlSchema}hexBinary") {
        lengthOfValue = value.length / 2
      }
      if (minLength.isDefined && lengthOfValue < minLength.get) {
        errors.addOne(
          ErrorWithoutContext(
            "minLength",
            s"value '$value' length less than minLength specified - $minLength"
          )
        )
      }
      if (maxLength.isDefined && lengthOfValue > maxLength.get) {
        errors.addOne(
          ErrorWithoutContext(
            "maxLength",
            s"value '$value' length greater than maxLength specified - $maxLength"
          )
        )
      }
      if (length.isDefined && lengthOfValue != length.get) {
        errors.addOne(
          ErrorWithoutContext(
            "length",
            s"value '$value' length different from length specified - ${length.get}"
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
          s"value '$value' less than minInclusive value '${minInclusive.get}'"
        )
      )
    }
    if (greaterThanMaxInclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "maxInclusive",
          s"value '$value' greater than maxInclusive value '${maxInclusive.get}'"
        )
      )
    }
    if (lessThanEqualToMinExclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "minExclusive",
          s"value '$value' less than or equal to minExclusive value '${minExclusive.get}'"
        )
      )
    }
    if (greaterThanEqualToMaxExclusive(value)) {
      errors.addOne(
        ErrorWithoutContext(
          "maxExclusive",
          s"value '$value' greater than or equal to maxExclusive value '${maxExclusive.get}'"
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
