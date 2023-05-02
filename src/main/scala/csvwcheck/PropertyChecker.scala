package csvwcheck
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{DateFormatError, MetadataError, NumberFormatError}
import csvwcheck.models.DateFormat
import csvwcheck.numberformatparser.LdmlNumberFormatParser
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import org.joda.time.DateTime

import java.lang
import java.net.{URI, URL}
import java.util.Map
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex
import scala.language.implicitConversions

object PropertyChecker {
  private val startsWithUnderscore = "^_:.*$".r
  private val containsColon = ".*:.*".r
  private val invalidValueWarning = "invalid_value"
  private val Bcp47Regular =
    "(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang)"
  private val Bcp47Irregular =
    "(en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)"
  private val Bcp47Grandfathered =
    "(?<grandfathered>" + Bcp47Irregular + "|" + Bcp47Regular + ")"
  private val Bcp47PrivateUse = "(x(-[A-Za-z0-9]{1,8})+)"
  private val Bcp47Singleton = "[0-9A-WY-Za-wy-z]"
  private val Bcp47Extension =
    "(?<extension>" + Bcp47Singleton + "(-[A-Za-z0-9]{2,8})+)"
  private val Bcp47Variant = "(?<variant>[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3})"
  private val Bcp47Region = "(?<region>[A-Za-z]{2}|[0-9]{3})"
  private val Bcp47Script = "(?<script>[A-Za-z]{4})"
  private val Bcp47Extlang = "(?<extlang>[A-Za-z]{3}(-[A-Za-z]{3}){0,2})"
  private val Bcp47Language =
    "(?<language>([A-Za-z]{2,3}(-" + Bcp47Extlang + ")?)|[A-Za-z]{4}|[A-Za-z]{5,8})"
  private val Bcp47Langtag =
    "(" + Bcp47Language + "(-" + Bcp47Script + ")?" + "(-" + Bcp47Region + ")?" + "(-" + Bcp47Variant + ")*" + "(-" + Bcp47Extension + ")*" + "(-" + Bcp47PrivateUse + ")?" + ")"
  private val Bcp47LanguagetagRegExp: Regex =
    ("^(" + Bcp47Grandfathered + "|" + Bcp47Langtag + "|" + Bcp47PrivateUse + ")").r

  private val prefixedPropertyPattern = "^[a-z]+:.*$".r

  type StringWarnings = Array[String]

  private val CsvWDataTypes = Array[String](
    "TableGroup",
    "Table",
    "Schema",
    "Column",
    "Dialect",
    "Template",
    "Datatype"
  )

  private val NameRegExp =
    "^([A-Za-z0-9]|(%[A-F0-9][A-F0-9]))([A-Za-z0-9_]|(%[A-F0-9][A-F0-9]))*$".r
  private val Properties: Map[String, (JsonNode, String, String) => Either[
    MetadataError,
    (JsonNode, Array[String], PropertyType.Value)
  ]] = Map(
    // Context Properties
    "@language" -> parseLanguageProperty(PropertyType.Context),
    "@base" -> parseLinkProperty(PropertyType.Context),
    // common properties
    "@id" -> parseLinkProperty(PropertyType.Common),
    "dialect" -> parseDialectProperty(PropertyType.Common),
    "notes" -> parseNotesProperty(PropertyType.Common),
    "suppressOutput" -> parseBooleanProperty(PropertyType.Common),
    // Inherited properties
    "aboutUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "datatype" -> parseDataTypeProperty(PropertyType.Inherited),
    "default" -> stringProperty(PropertyType.Inherited),
    "lang" -> parseLanguageProperty(PropertyType.Inherited),
    "null" -> nullProperty(PropertyType.Inherited),
    "ordered" -> parseBooleanProperty(PropertyType.Inherited),
    "propertyUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "required" -> parseBooleanProperty(PropertyType.Inherited),
    "separator" -> separatorProperty(PropertyType.Inherited),
    "textDirection" -> textDirectionProperty(PropertyType.Inherited),
    "valueUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    // Table properties
    "tableSchema" -> tableSchemaProperty(PropertyType.Table),
    "transformations" -> transformationsProperty(PropertyType.Table),
    "url" -> parseLinkProperty(PropertyType.Table),
    // Schema Properties
    "columns" -> columnsProperty(PropertyType.Schema),
    "foreignKeys" -> foreignKeysProperty(PropertyType.Schema),
    "primaryKey" -> columnReferenceProperty(PropertyType.Schema),
    "rowTitles" -> columnReferenceProperty(PropertyType.Schema),
    // Column level properties
    "name" -> nameProperty(PropertyType.Column),
    "titles" -> naturalLanguageProperty(PropertyType.Column),
    "virtual" -> parseBooleanProperty(PropertyType.Column),
    // Dialect Properties
    "commentPrefix" -> stringProperty(PropertyType.Dialect),
    "delimiter" -> stringProperty(PropertyType.Dialect),
    "doubleQuote" -> parseBooleanProperty(PropertyType.Dialect),
    "encoding" -> encodingProperty(PropertyType.Dialect),
    "header" -> parseBooleanProperty(PropertyType.Dialect),
    "headerRowCount" -> numericProperty(PropertyType.Dialect),
    "lineTerminators" -> arrayProperty(PropertyType.Dialect),
    "quoteChar" -> stringProperty(PropertyType.Dialect),
    "skipBlankRows" -> parseBooleanProperty(PropertyType.Dialect),
    "skipColumns" -> numericProperty(PropertyType.Dialect),
    "skipInitialSpace" -> parseBooleanProperty(PropertyType.Dialect),
    "skipRows" -> numericProperty(PropertyType.Dialect),
    "trim" -> trimProperty(PropertyType.Dialect),
    // Transformation properties
    "scriptFormat" -> scriptFormatProperty(PropertyType.Transformation),
    "source" -> sourceProperty(PropertyType.Transformation),
    "targetFormat" -> targetFormatProperty(PropertyType.Transformation),
    // Foreign Key Properties
    "columnReference" -> columnReferenceProperty(PropertyType.ForeignKey),
    "reference" -> referenceProperty(PropertyType.ForeignKey),
    // foreignKey reference properties
    "resource" -> resourceProperty(PropertyType.ForeignKeyReference),
    "schemaReference" -> schemaReferenceProperty(
      PropertyType.ForeignKeyReference
    )
  )

  def nullProperty(
                    csvwPropertyType: PropertyType.Value
                  ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    if (value.isTextual) {
      (value, Array[String](), csvwPropertyType)
    } else if (value.isArray) {
      var values = Array[String]()
      var warnings = Array[String]()
      for (x <- value.elements().asScala) {
        x match {
          case xs: TextNode => values = values :+ xs.asText()
          case _ => warnings = warnings :+ PropertyChecker.invalidValueWarning
        }
      }
      val arrayNode: ArrayNode = objectMapper.valueToTree(values)
      (arrayNode, warnings, csvwPropertyType)
    } else {
      val warnings = if (value.isNull) {
        Array[String]()
      } else {
        Array[String](PropertyChecker.invalidValueWarning)
      }
      val arrayNodeToReturn = JsonNodeFactory.instance.arrayNode()
      arrayNodeToReturn.add("")
      (
        arrayNodeToReturn,
        warnings,
        csvwPropertyType
      )
    }
  }
  }

  def separatorProperty(
                         csvwPropertyType: PropertyType.Value
                       ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case s if s.isTextual || s.isNull =>
        (s, Array[String](), csvwPropertyType)
      case _ =>
        (
          NullNode.getInstance(),
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def parseLanguageProperty(
                        csvwPropertyType: PropertyType.Value
                      ): (JsonNode, String, String) => Either[
    MetadataError,
    (
      JsonNode,
        Array[String],
        PropertyType.Value
      )
  ] = { (value, _, _) => {
    value match {
      case s: TextNode
        if PropertyChecker.Bcp47LanguagetagRegExp.matches(s.asText) => Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode(""),
            Array[String](PropertyChecker.invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  def parseDataTypeObject(objectNode: ObjectNode, baseUrl: String, lang: String): Either[MetadataError, (ObjectNode, StringWarnings)] = {
    objectNode.fields.asScala
      .map(keyAndValue => {
        val key = keyAndValue.getKey
        val valueNode = keyAndValue.getValue

        key match {
          case "@id" => parseDataTypeObjectIdNode(baseUrl, lang, valueNode).map({ case (idNode, warns) => ("@id", idNode, warns) })
          case "base" =>
            val baseValue = valueNode.asText()
            if (XsdDataTypes.types.contains(baseValue)) {
              Right(("base", Some(new TextNode(baseValue)), Array[String]()))
            } else {
              Right(("base", Some(new TextNode(XsdDataTypes.types("string"))), Array("invalid_datatype_base")))
            }
          case _ => Right((key, Some(valueNode), Array[String]())) // todo: Is this right?
        }
      })
      .toObjectNodeAndStringWarnings
      .map({case (objectNode, stringWarnings) =>
        // Make sure that the `base` node is set.
        val baseNode = objectNode.path("base")
        if (baseNode.isMissingNode) {
          (objectNode.deepCopy().set("base", new TextNode(XsdDataTypes.types("string"))), stringWarnings)
        } else {
          (objectNode, stringWarnings)
        }
      })
  }

  def parseDataTypeMinMaxValues(inputs: (ObjectNode, StringWarnings)): Either[MetadataError, (ObjectNode, StringWarnings)] = {
    val (dataTypeNode, stringWarnings) = inputs
    val baseDataType = dataTypeNode.get("base").asText

    val minimumNode = dataTypeNode.path("minimum")
    val minInclusiveNode = dataTypeNode.path("minInclusive")
    val minExclusiveNode = dataTypeNode.path("minExclusive")
    val maximumNode = dataTypeNode.path("maximum")
    val maxInclusiveNode = dataTypeNode.path("maxInclusive")
    val maxExclusiveNode = dataTypeNode.path("maxExclusive")

    if (PropertyCheckerConstants.DateFormatDataTypes.contains(baseDataType) || PropertyCheckerConstants.NumericFormatDataTypes.contains(baseDataType)) {
      // Date and Numeric types are permitted min/max/etc. values
      parseMinMaxRanges(dataTypeNode, baseDataType, minimumNode, maximumNode, minInclusiveNode, minExclusiveNode, maxInclusiveNode, maxExclusiveNode, stringWarnings)
    } else {
      // Only date and numeric types as permitted min/max/etc. values

      val offendingNodes = Array(minimumNode, minInclusiveNode, minExclusiveNode, maximumNode, maxInclusiveNode, maxExclusiveNode).filter(node => !node.isMissingNode)
      if (offendingNodes.nonEmpty) {
        Left(MetadataError(
          "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types"
        ))
      }
      Right(inputs)
    }
  }

  private def parseMinMaxRanges(dataTypeNode: ObjectNode, baseDataType: String, minimumNode: JsonNode, maximumNode: JsonNode, minInclusiveNode: JsonNode, minExclusiveNode: JsonNode, maxInclusiveNode: JsonNode, maxExclusiveNode: JsonNode, stringWarnings: StringWarnings):
  Either[MetadataError, (ObjectNode, StringWarnings)]= {
    if (!minimumNode.isMissingNode) {
      dataTypeNode.put("minInclusive", minimumNode.asText())
      dataTypeNode.remove("minimum")
    }

    if (!maximumNode.isMissingNode) {
      dataTypeNode.put("maxInclusive", maximumNode.asText())
      dataTypeNode.remove("maximum")
    }

    val minInclusive: Option[String] = getDataTypeRangeConstraint(minInclusiveNode)
    val maxInclusive: Option[String] = getDataTypeRangeConstraint(maxInclusiveNode)
    val minExclusive: Option[String] = getDataTypeRangeConstraint(minExclusiveNode)
    val maxExclusive: Option[String] = getDataTypeRangeConstraint(maxExclusiveNode)

    (minInclusive, minExclusive, maxInclusive, maxExclusive) match {
      case (Some(minI), Some(minE), _, _) =>
        Left(MetadataError(
          s"datatype cannot specify both minimum/minInclusive ($minI) and minExclusive ($minE)"
        ))
      case (_, _, Some(maxI), Some(maxE)) =>
        Left(MetadataError(
          s"datatype cannot specify both maximum/maxInclusive ($maxI) and maxExclusive ($maxE)"
        ))
      case _ =>
        if (PropertyCheckerConstants.NumericFormatDataTypes.contains(baseDataType)) {
          parseMinMaxNumericRanges(dataTypeNode, stringWarnings, minInclusive, maxInclusive, minExclusive, maxExclusive)
        } else if (
          PropertyCheckerConstants.DateFormatDataTypes.contains(baseDataType)
        ) {
          parseMinMaxDateTimeRanges(dataTypeNode, stringWarnings, minInclusive, maxInclusive, minExclusive, maxExclusive)
        }
        throw new IllegalArgumentException(s"Base data type was neither numeric note date/time - $baseDataType")
    }
  }

  private def parseMinMaxDateTimeRanges(dataTypeNode: ObjectNode, stringWarnings: StringWarnings, minInclusive: Option[String], maxInclusive: Option[String], minExclusive: Option[String], maxExclusive: Option[String]) = {
    (
      minInclusive.map(DateTime.parse),
      minExclusive.map(DateTime.parse),
      maxInclusive.map(DateTime.parse),
      maxExclusive.map(DateTime.parse)
    ) match {
      case (Some(minI), _, Some(maxI), _)
        if minI.getMillis > maxI.getMillis =>
        Left(MetadataError(
          s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
        ))
      case (Some(minI), _, _, Some(maxE))
        if minI.getMillis >= maxE.getMillis =>
        Left(MetadataError(
          s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
        ))
      case (_, Some(minE), _, Some(maxE))
        if minE.getMillis > maxE.getMillis =>
        Left(MetadataError(
          s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
        ))
      case (_, Some(minE), Some(maxI), _)
        if minE.getMillis >= maxI.getMillis =>
        Left(MetadataError(
          s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
        ))
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }

  private def parseMinMaxNumericRanges(dataTypeNode: ObjectNode, stringWarnings: StringWarnings, minInclusive: Option[String], maxInclusive: Option[String], minExclusive: Option[String], maxExclusive: Option[String]) = {
    (
      minInclusive.map(BigDecimal(_)),
      minExclusive.map(BigDecimal(_)),
      maxInclusive.map(BigDecimal(_)),
      maxExclusive.map(BigDecimal(_))
    ) match {
      case (Some(minI), _, Some(maxI), _) if minI > maxI =>
        Left(MetadataError(
          s"datatype minInclusive ($minI) cannot be greater than maxInclusive ($maxI)"
        ))
      case (Some(minI), _, _, Some(maxE)) if minI >= maxE =>
        Left(MetadataError(
          s"datatype minInclusive ($minI) cannot be greater than or equal to maxExclusive ($maxE)"
        ))
      case (_, Some(minE), _, Some(maxE)) if minE > maxE =>
        Left(MetadataError(
          s"datatype minExclusive ($minE) cannot be greater than or equal to maxExclusive ($maxE)"
        ))
      case (_, Some(minE), Some(maxI), _) if minE >= maxI =>
        Left(MetadataError(
          s"datatype minExclusive ($minE) cannot be greater than maxInclusive ($maxI)"
        ))
      case _ => Right((dataTypeNode, stringWarnings))
    }
  }

  def validateRegEx(regexCandidate: String): Either[Exception, Unit] =
    try {
      regexCandidate.r
      Right()
    } catch {
      case e: Exception =>
        Left(e)
    }

  def parseDataTypeFormat(formatNode: JsonNode, baseDataType: String): (Option[JsonNode], StringWarnings) = {
    if (
      PropertyCheckerConstants.RegExpFormatDataTypes.contains(baseDataType)
    ) {
      val regExFormat = formatNode.asText
      validateRegEx(regExFormat) match {
        case Right(()) => (Some(formatNode), Array.empty)
        case Left(e) => (None, Array(s"invalid_regex '$regExFormat' - ${e.getMessage}"))
      }
    } else if (
      PropertyCheckerConstants.NumericFormatDataTypes.contains(baseDataType)
    ) {
      val (formatNode, stringWarnings) = parseDataTypeFormatNumeric(formatNode)
      (Some(formatNode), stringWarnings)
    } else if (baseDataType == "http://www.w3.org/2001/XMLSchema#boolean") {
      formatNode match {
        case formatTextNode: TextNode =>
          val formatValues = formatNode.asText.split("""\|""")
          if (formatValues.length != 2) {
            (None, Array(s"invalid_boolean_format '$formatNode'"))
          } else {
            (Some(formatTextNode), Array.empty)
          }
        case _ =>
          // Boolean formats should always be textual
          (None, Array(s"invalid_boolean_format '$formatNode'"))
      }
    } else if (
      PropertyCheckerConstants.DateFormatDataTypes.contains(baseDataType)
    ) {
      formatNode match {
        case formatTextNode: TextNode =>
          val dateFormatString = formatTextNode.asText()
          try {
            val format = DateFormat(Some(dateFormatString), baseDataType).format
            if (format.isDefined) {
              (Some(new TextNode(format.get)), Array.empty)
            } else {
              (None, Array(s"invalid_date_format '$dateFormatString'"))
            }
          } catch {
            case _: DateFormatError =>
              (None, Array(s"invalid_date_format '$dateFormatString'"))
          }
        case _ =>
          (None, Array(s"invalid_date_format '$formatNode'"))
      }
    } else {
      throw new IllegalArgumentException(s"Unhandled format node $formatNode")
    }
  }

  def parseDataTypeProperty(
                             csvwPropertyType: PropertyType.Value
                           ): (JsonNode, String, String) => Either[MetadataError, (
    ObjectNode,
      Array[String],
      PropertyType.Value
    )] = { (value, baseUrl, lang) => {
      initialDataTypePropertyParse(value, baseUrl, lang)
        .flatMap(parseDataTypeLengths)
        .flatMap(parseDataTypeMinMaxValues)
        .map({ case (dataTypeNode, _) =>
          dataTypeNode.getMaybeNode("format")
            .map(formatNode => {
              val baseDataType = dataTypeNode.get("base").asText()
              val (formatNodeReplacement, stringWarnings) = parseDataTypeFormat(formatNode, baseDataType)
              val parsedDataTypeNode = formatNodeReplacement match {
                case Some(newNode) => dataTypeNode.deepCopy().set("format", newNode)
                case None => dataTypeNode.deepCopy().remove("format").asInstanceOf[ObjectNode]
              }
              (parsedDataTypeNode, stringWarnings)
            })
            .getOrElse((dataTypeNode, Array.empty))
        })
        .map({ case (dataTypeNode: Any, stringWarnings) => (dataTypeNode, stringWarnings, csvwPropertyType) })
    }
  }

  private def parseDataTypeLengths(inputs: (ObjectNode, StringWarnings)): Either[MetadataError, (ObjectNode, StringWarnings)] = {
    val (dataTypeNode: ObjectNode, _) = inputs

    val baseDataType = dataTypeNode.get("base").asText()

    val lengthNode = dataTypeNode.path("length")
    val minLengthNode = dataTypeNode.path("minLength")
    val maxLengthNode = dataTypeNode.path("maxLength")

    if (
      PropertyCheckerConstants.StringDataTypes.contains(baseDataType) || PropertyCheckerConstants.BinaryDataTypes.contains(baseDataType)
    ) {
      // String and Binary data types are permitted length/minLength/maxLength fields.
      if (
        !lengthNode.isMissingNode && !minLengthNode.isMissingNode && lengthNode.asInt < minLengthNode.asInt
      ) {
        Left(MetadataError(
          s"datatype length (${lengthNode.asInt}) cannot be less than minLength (${minLengthNode.asInt})"
        ))
      } else if (
        !lengthNode.isMissingNode && !maxLengthNode.isMissingNode && lengthNode.asInt > maxLengthNode.asInt
      ) {
        Left(MetadataError(
          s"datatype length (${lengthNode.asInt}) cannot be more than maxLength (${maxLengthNode.asInt})"
        ))
      } else if (
        !minLengthNode.isMissingNode && !maxLengthNode.isMissingNode && minLengthNode.asInt > maxLengthNode.asInt
      ) {
        Left(MetadataError(
          s"datatype minLength (${minLengthNode.asInt}) cannot be more than maxLength (${maxLengthNode.asInt})"
        ))
      } else {
        Right(inputs)
      }
    } else {
      // length, minLength and maxLength are only permitted on String and Binary data types.
      if (!lengthNode.isMissingNode) {
        Left(MetadataError(
          s"Data types based on $baseDataType cannot have a length facet"
        ))
      } else if (!minLengthNode.isMissingNode) {
        Left(MetadataError(
          s"Data types based on $baseDataType cannot have a minLength facet"
        ))
      } else if (!maxLengthNode.isMissingNode) {
        Left(MetadataError(
          s"Data types based on $baseDataType cannot have a maxLength facet"
        ))
      } else {
        Right(inputs)
      }
    }
  }

  private def initialDataTypePropertyParse(value: JsonNode, baseUrl: String, lang: String): Either[MetadataError, (ObjectNode, StringWarnings)] = {
    value match {
      case dataTypeObjectNode: ObjectNode => parseDataTypeObject(dataTypeObjectNode, baseUrl, lang)
      case x: TextNode if XsdDataTypes.types.contains(x.asText()) =>
        Right((objectMapper.createObjectNode
          .put("@id", XsdDataTypes.types(x.asText())), Array[String]()))
      case _: TextNode =>
        Right((objectMapper.createObjectNode
          .put("@id", XsdDataTypes.types("string")), Array(PropertyChecker.invalidValueWarning))
    }
  }

  private def parseDataTypeObjectIdNode(baseUrl: String, lang: String, valueNode: JsonNode): Either[MetadataError, (Option[TextNode], StringWarnings)] = {
    val idValue = valueNode.asText()
    if (XsdDataTypes.types.values.toList.contains(idValue)) {
      Left(MetadataError(
        s"datatype @id must not be the id of a built-in datatype ($idValue)"
      ))
    } else {
      parseLinkProperty(PropertyType.Common)(
        valueNode,
        baseUrl,
        lang
      ).map({
        case (linkNode, warns@Array(), _) => (Some(linkNode), warns)
        case (_, warns, _) => (None, warns)
      })
    }
  }

  def parseLinkProperty(
                    csvwPropertyType: PropertyType.Value
                  ): (JsonNode, String, String) => Either[
    MetadataError,
    (
      TextNode,
        Array[String],
        PropertyType.Value
      )
  ] = { (v, baseUrl, _) => {
    v match {
      case urlNode: TextNode =>
        val urlValue = urlNode.asText()
        if (PropertyChecker.startsWithUnderscore.matches(urlValue)) {
          Left(MetadataError(s"URL ${urlValue} starts with _:"))
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
            Array[String](PropertyChecker.invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def getDataTypeRangeConstraint(constraintNode: JsonNode): Option[String] =
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

  def parseDataTypeFormatNumeric(formatNode: JsonNode): (ObjectNode, StringWarnings) = {
    formatNode match {
      case _: TextNode =>
        val formatObjectNode = JsonNodeFactory.instance.objectNode()
        parseDataTypeFormatNumeric(formatObjectNode.set("pattern", formatNode.deepCopy()))
      case formatObjectNode: ObjectNode =>
        val groupChar = formatObjectNode.getMaybeNode("groupChar").map(_.asText.charAt(0))
        val decimalChar = formatObjectNode.getMaybeNode("decimalChar").map(_.asText.charAt(0))
        try {
          formatObjectNode.getMaybeNode("pattern")
            .map(_.asText)
            .map(pattern =>
              LdmlNumberFormatParser(
                groupChar.getOrElse(','),
                decimalChar.getOrElse('.')
              ).getParserForFormat(pattern)
            )
            .get

          (formatObjectNode, Array.empty)
        } catch {
          case e: NumberFormatError =>
            (formatObjectNode.deepCopy().remove("pattern").asInstanceOf[ObjectNode], Array(s"invalid_number_format - ${e.getMessage}"))
        }
      case _ => throw new IllegalArgumentException(s"Unhandled numeric data type format $formatNode")
    }
  }

  def tableSchemaProperty(
                           csvwPropertyType: PropertyType.Value
                         ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = {
    def tableSchemaPropertyInternal(
                                     value: JsonNode,
                                     baseUrl: String,
                                     lang: String
                                   ): (JsonNode, Array[String], PropertyType.Value) = {
      var schemaBaseUrl = new URL(baseUrl)
      var schemaLang = lang

      var schemaJson: ObjectNode = null
      if (value.isTextual) {
        val schemaUrl = new URL(new URL(baseUrl), value.asText())
        schemaJson = objectMapper.readTree(schemaUrl).asInstanceOf[ObjectNode]
        if (!schemaJson.path("@id").isMissingNode) {
          // Do something here as object node put method doesn't allow uri object
          val absoluteSchemaUrl =
            new URL(schemaUrl, schemaJson.get("@id").asText())
          schemaJson.put("@id", absoluteSchemaUrl.toString)
        } else {
          schemaJson.put("@id", schemaUrl.toString)
        }
        val (newSchemaBaseUrl, newSchemaLang) =
          fetchSchemaBaseUrlAndLangAndRemoveContext(
            schemaJson,
            schemaUrl,
            schemaLang
          )
        schemaBaseUrl = newSchemaBaseUrl
        schemaLang = newSchemaLang
      } else if (value.isObject) {
        schemaJson = value.deepCopy()
      } else {
        return (
          NullNode.getInstance(),
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
      }

      var warnings = Array[String]()
      val fieldsAndValues = Array.from(schemaJson.fields.asScala)
      for (fieldAndValue <- fieldsAndValues) {
        warnings ++= validateObjectAndUpdateSchemaJson(
          schemaJson,
          schemaBaseUrl,
          schemaLang,
          fieldAndValue.getKey,
          fieldAndValue.getValue
        )
      }
      (schemaJson, warnings, PropertyType.Table)
    }

    tableSchemaPropertyInternal
  }

  def validateObjectAndUpdateSchemaJson(
                                         schemaJson: ObjectNode,
                                         schemaBaseUrl: URL,
                                         schemaLang: String,
                                         property: String,
                                         value: JsonNode
                                       ): Array[String] = {
    var warnings = Array[String]()
    if (property == "@id") {
      val matcher =
        PropertyChecker.startsWithUnderscore.pattern.matcher(value.asText())
      if (matcher.matches) {
        throw MetadataError(s"@id ${value.asText} starts with _:")
      }
    } else if (property == "@type") {
      if (value.asText() != "Schema") {
        throw MetadataError("@type of schema is not 'Schema'")
      }
    } else {
      val (validatedV, warningsForP, propertyType) =
        checkProperty(property, value, schemaBaseUrl.toString, schemaLang)
      warnings ++= warningsForP
      if (
        (propertyType == PropertyType.Schema || propertyType == PropertyType.Inherited) && warningsForP.isEmpty
      ) {
        schemaJson.set(property, validatedV)
      } else {
        schemaJson.remove(property)
        if (
          propertyType != PropertyType.Schema && propertyType != PropertyType.Inherited
        ) {
          warnings :+= "invalid_property"
        }
      }
    }
    warnings
  }

  def fetchSchemaBaseUrlAndLangAndRemoveContext(
                                                 schemaJson: ObjectNode,
                                                 schemaBaseUrl: URL,
                                                 schemaLang: String
                                               ): (URL, String) = {
    if (!schemaJson.path("@context").isMissingNode) {
      if (schemaJson.isArray && schemaJson.size > 1) {
        val secondContextElement =
          Array.from(schemaJson.get("@context").elements.asScala).apply(1)
        val maybeBaseNode = secondContextElement.path("@base")
        val newSchemaBaseUrl = if (!maybeBaseNode.isMissingNode) {
          new URL(schemaBaseUrl, maybeBaseNode.asText())
        } else {
          schemaBaseUrl
        }
        val languageNode = secondContextElement.path("@language")
        val newSchemaLang = if (!languageNode.isMissingNode) {
          languageNode.asText()
        } else {
          schemaLang
        }
        schemaJson.remove("@context")
        return (newSchemaBaseUrl, newSchemaLang)
      }
      schemaJson.remove("@context")
    }
    (schemaBaseUrl, schemaLang)
  }

  def foreignKeysProperty(
                           csvwPropertyType: PropertyType.Value
                         ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, baseUrl, lang) => {
    var foreignKeys = Array[JsonNode]()
    var warnings = Array[String]()
    value match {
      case xs: ArrayNode =>
        val arrayNodes = Array.from(xs.elements().asScala)
        for (foreignKey <- arrayNodes) {
          val (fk, warn) = foreignKeyCheckIfValid(foreignKey, baseUrl, lang)
          foreignKeys = foreignKeys :+ fk
          warnings = Array.concat(warnings, warn)
        }
      case _ => warnings = warnings :+ PropertyChecker.invalidValueWarning
    }
    val arrayNode: ArrayNode = objectMapper.valueToTree(foreignKeys)
    (arrayNode, warnings, csvwPropertyType)
  }
  }

  def foreignKeyCheckIfValid(
                              foreignKey: JsonNode,
                              baseUrl: String,
                              lang: String
                            ): (JsonNode, Array[String]) = {
    var warnings = Array[String]()
    foreignKey match {
      case _: ObjectNode =>
        val foreignKeyCopy = foreignKey
          .deepCopy()
          .asInstanceOf[ObjectNode]
        val foreignKeysElements = Array.from(foreignKeyCopy.fields().asScala)
        for (f <- foreignKeysElements) {
          val p = f.getKey
          val matcher = PropertyChecker.containsColon.pattern.matcher(p)
          if (matcher.matches()) {
            throw MetadataError(
              "foreignKey includes a prefixed (common) property"
            )
          }
          val (value, w, typeString) =
            checkProperty(p, f.getValue, baseUrl, lang)
          if (typeString == PropertyType.ForeignKey && w.isEmpty) {
            foreignKeyCopy.set(p, value)
          } else {
            foreignKeyCopy.remove(p)
            warnings = warnings :+ PropertyChecker.invalidValueWarning
            warnings = Array.concat(warnings, w)
          }
        }
        (foreignKeyCopy, warnings)
      case _ =>
        val foreignKeyCopy = JsonNodeFactory.instance.objectNode()
        warnings = warnings :+ "invalid_foreign_key"
        (foreignKeyCopy, warnings)
    }
  }

  def referenceProperty(
                         csvwPropertyType: PropertyType.Value
                       ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, baseUrl, lang) => {
    value match {
      case obj: ObjectNode =>
        val valueCopy = obj.deepCopy()
        var warnings = Array[String]()
        val valueCopyElements = Array.from(valueCopy.fields().asScala)
        for (e <- valueCopyElements) {
          val p = e.getKey
          val v = e.getValue
          val matcher = PropertyChecker.containsColon.pattern.matcher(p)
          // Check if property is included in the valid properties for a foreign key object
          if (
            Array[String]("resource", "schemaReference", "columnReference")
              .contains(p)
          ) {
            val (newValue, warning, _) =
              checkProperty(p, v, baseUrl, lang)
            if (warning.isEmpty) {
              valueCopy.set(p, newValue)
            } else {
              valueCopy.remove(p)
              warnings = Array.concat(warnings, warning)
            }
          } else if (matcher.matches()) {
            throw MetadataError(
              s"foreignKey reference ($p) includes a prefixed (common) property"
            )
          } else {
            valueCopy.remove(p)
            warnings = warnings :+ PropertyChecker.invalidValueWarning
          }
        }
        if (valueCopy.path("columnReference").isMissingNode) {
          throw MetadataError(
            "foreignKey reference columnReference is missing"
          )
        }
        if (
          valueCopy
            .path("resource")
            .isMissingNode && valueCopy.path("schemaReference").isMissingNode
        ) {
          throw MetadataError(
            "foreignKey reference does not have either resource or schemaReference"
          ) // Should have at least one of them, else it is an error
        }
        if (
          !valueCopy
            .path("resource")
            .isMissingNode && !valueCopy.path("schemaReference").isMissingNode
        ) {
          throw MetadataError(
            "foreignKey reference has both resource and schemaReference"
          )
        }
        (valueCopy, warnings, csvwPropertyType)
      case _ =>
        throw MetadataError("foreignKey reference is not an object")
    }
  }
  }

  def parseUriTemplateProperty(
                           csvwPropertyType: PropertyType.Value
                         ): (JsonNode, String, String) => Either[MetadataError, (
    JsonNode,
      Array[String],
      PropertyType.Value
    )] = { (value, _, _) => {
    value match {
      case s: TextNode => Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right((
          new TextNode(""),
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        ))
    }
  }
  }

  def textDirectionProperty(
                             csvwPropertyType: PropertyType.Value
                           ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case s: TextNode
        if Array[String]("ltr", "rtl", "inherit").contains(s.asText()) =>
        (value, Array[String](), PropertyType.Inherited)
      case _ =>
        (
          new TextNode(PropertyType.Inherited.toString),
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def getValidTitlesWithWarnings(
                                  a: ArrayNode
                                ): (Array[String], Array[String]) = {
    var validTitles = List[String]()
    var warnings = List[String]()

    val arrayNodeElements = Array.from(a.elements().asScala)
    for (element <- arrayNodeElements) {
      element match {
        case s: TextNode => validTitles :+= s.asText()
        case _ =>
          warnings =
            warnings :+ a.toPrettyString + " is invalid, textual elements expected"
          warnings = warnings :+ PropertyChecker.invalidValueWarning
      }
    }
    (validTitles.toArray, warnings.toArray)
  }

  def naturalLanguageProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, lang) => {
    var warnings = Array[String]()
    value match {
      case s: TextNode =>
        val returnObject = JsonNodeFactory.instance.objectNode()
        val arrayNode = JsonNodeFactory.instance.arrayNode()
        arrayNode.add(s.asText())
        returnObject.set(lang, arrayNode)
        (returnObject, Array[String](), csvwPropertyType)
      case a: ArrayNode =>
        val (validTitles, titleWarnings) = getValidTitlesWithWarnings(a)
        warnings = warnings ++ titleWarnings
        val returnObject = JsonNodeFactory.instance.objectNode()
        val arrayNode: ArrayNode =
          objectMapper.valueToTree(validTitles)
        returnObject.set(lang, arrayNode)
        (returnObject, warnings, csvwPropertyType)
      case o: ObjectNode =>
        val (valueCopy, w) = processNaturalLanguagePropertyObject(o)
        warnings = Array.concat(warnings, w)
        (valueCopy, warnings, csvwPropertyType)
      case _ =>
        (
          NullNode.getInstance(),
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def processNaturalLanguagePropertyObject(
                                            value: ObjectNode
                                          ): (ObjectNode, Array[String]) = {
    val valueCopy = value.deepCopy()
    var warnings = Array[String]()
    val fieldsAndValues = Array.from(valueCopy.fields.asScala)
    for (fieldAndValue <- fieldsAndValues) {
      val elementKey = fieldAndValue.getKey
      val matcher =
        PropertyChecker.Bcp47LanguagetagRegExp.pattern.matcher(elementKey)
      if (matcher.matches()) {
        var validTitles = Array[String]()
        fieldAndValue.getValue match {
          case s: TextNode => validTitles :+= s.asText()
          case a: ArrayNode =>
            val (titles, titleWarnings) = getValidTitlesWithWarnings(a)
            validTitles ++= titles
            warnings ++= titleWarnings
          case _ =>
            warnings =
              warnings :+ fieldAndValue.getValue.toPrettyString + " is invalid, array or textual elements expected"
            warnings = warnings :+ PropertyChecker.invalidValueWarning
        }
        val validTitlesArrayNode: ArrayNode =
          objectMapper.valueToTree(validTitles)
        valueCopy.set(elementKey, validTitlesArrayNode)
      } else {
        valueCopy.remove(elementKey)
        warnings = warnings :+ "invalid_language"
      }
    }
    (valueCopy, warnings)
  }

  def nameProperty(
                    csvwPropertyType: PropertyType.Value
                  ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case s: TextNode =>
        val matcher = PropertyChecker.NameRegExp.pattern.matcher(s.asText())
        if (matcher.matches()) {
          (s, Array[String](), PropertyType.Column)
        } else {
          (
            NullNode.instance,
            Array[String](PropertyChecker.invalidValueWarning),
            csvwPropertyType
          )
        }
      case _ =>
        (
          NullNode.instance,
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def encodingProperty(
                        csvwPropertyType: PropertyType.Value
                      ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case s: TextNode
        if PropertyCheckerConstants.ValidEncodings.contains(s.asText()) =>
        (s, Array[String](), csvwPropertyType)
      case _ =>
        (
          NullNode.instance,
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def arrayProperty(
                     csvwPropertyType: PropertyType.Value
                   ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case a: ArrayNode => (a, Array[String](), csvwPropertyType)
      case _ =>
        (
          BooleanNode.getFalse,
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def trimProperty(
                    csvwPropertyType: PropertyType.Value
                  ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    var valueCopy: JsonNode = value.deepCopy()
    valueCopy match {
      case b: BooleanNode =>
        if (b.booleanValue) {
          valueCopy = new TextNode("true")
        } else {
          valueCopy = new TextNode("false")
        }
      case _ =>
    }
    if (
      Array[String]("true", "false", "start", "end")
        .contains(valueCopy.asText())
    ) {
      (valueCopy, Array[String](), csvwPropertyType)
    } else {
      (
        new TextNode("false"),
        Array[String](PropertyChecker.invalidValueWarning),
        csvwPropertyType
      )
    }
  }
  }

  def columnsProperty(
                       csvwPropertyType: PropertyType.Value
                     ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    (value, Array[String](), csvwPropertyType)
  }
  }

  def columnReferenceProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    value match {
      case s: TextNode =>
        val arrayNode = JsonNodeFactory.instance.arrayNode()
        arrayNode.add(s)
        (arrayNode, Array[String](), csvwPropertyType)
      case a: ArrayNode => (a, Array[String](), csvwPropertyType)
    }
  }
  }

  def targetFormatProperty(
                            csvwPropertyType: PropertyType.Value
                          ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    (value, Array[String](), csvwPropertyType)
  }
  }

  def scriptFormatProperty(
                            csvwPropertyType: PropertyType.Value
                          ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    (value, Array[String](), csvwPropertyType)
  }
  }

  def sourceProperty(
                      csvwPropertyType: PropertyType.Value
                    ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    (value, Array[String](), csvwPropertyType)
  }
  }

  def resourceProperty(
                        csvwPropertyType: PropertyType.Value
                      ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    (value, Array[String](), csvwPropertyType)
  }
  }

  def schemaReferenceProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, baseUrl, _) => {
    val url = new URL(new URL(baseUrl), value.asText()).toString
    // Don't know how to place a URI object in JsonNode, keeping the text value as of now
    (new TextNode(url), Array[String](), csvwPropertyType)
  }
  }

  def parseDialectProperty(
                       csvwPropertyType: PropertyType.Value
                     ): (JsonNode, String, String) => Either[
    MetadataError,
    (
      JsonNode,
        Array[String],
        PropertyType.Value
      )
  ] = { (value, baseUrl, lang) => {
    value match {
      case objectNode: ObjectNode =>
        objectNode.fields.asScala
          .map(fieldAndValue =>
            parseDialectObjectProperty(
              baseUrl,
              lang,
              fieldAndValue.getKey(),
              fieldAndValue.getValue()
            )
          ).toObjectNodeAndStringWarnings
          .map({
            case (objectNode, warnings) =>
              (objectNode, warnings, csvwPropertyType)
          })
      case _ =>
        // May be we might need to support dialect property of type other than ObjectNode.
        //  The dialect of a table is an object property. It could be provided as a URL that indicates
        //  a commonly used dialect, like this:
        //  "dialect": "http://example.org/tab-separated-values"
        Right(
          (
            NullNode.instance,
            Array[String](PropertyChecker.invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseDialectObjectProperty(
                                          baseUrl: String,
                                          lang: String,
                                          key: String,
                                          valueNode: JsonNode
                                        ): Either[MetadataError, (String, Option[JsonNode], StringWarnings)] = {
    key match {
      case "@id" =>
        if (PropertyChecker.startsWithUnderscore.matches(valueNode.asText())) {
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
        checkProperty(key, valueNode, baseUrl, lang)
          .map({
            case (parsedValueNode, propertyWarnings, propertyType) =>
              if (
                propertyType == PropertyType.Dialect && propertyWarnings.isEmpty
              ) {
                (key, Some(parsedValueNode), Array())
              } else {
                val warnings =
                  if (propertyType != PropertyType.Dialect)
                    Array("invalid_property")
                  else Array[String]()
                (key, None, warnings)
              }
          })
    }
  }

  def transformationsProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, baseUrl, lang) => {
    var warnings = Array[String]()
    val transformationsToReturn = JsonNodeFactory.instance.arrayNode()
    value match {
      case a: ArrayNode =>
        val transformationsArr = Array.from(a.elements().asScala)
        for ((transformation, index) <- transformationsArr.zipWithIndex) {
          transformation match {
            case o: ObjectNode =>
              val w = processTransformationObjectAndReturnWarnings(
                transformationsToReturn,
                o,
                index,
                baseUrl,
                lang
              )
              warnings = Array.concat(warnings, w)
            case _ => warnings = warnings :+ "invalid_transformation"
          }
        }
      case _ => warnings = warnings :+ PropertyChecker.invalidValueWarning
    }
    (transformationsToReturn, warnings, csvwPropertyType)
  }
  }

  def processTransformationObjectAndReturnWarnings(
                                                    transformationsToReturn: ArrayNode,
                                                    transformationsMainObject: ObjectNode,
                                                    index: Int,
                                                    baseUrl: String,
                                                    lang: String
                                                  ): Array[String] = {
    val transformationObjects =
      Array.from(transformationsMainObject.fields().asScala)
    var warnings = Array[String]()
    for (elem <- transformationObjects) {
      val property = elem.getKey
      val value = elem.getValue
      property match {
        case "@id" =>
          val matcher =
            PropertyChecker.startsWithUnderscore.pattern.matcher(value.asText())
          if (matcher.matches) {
            throw MetadataError(
              s"transformations[$index].@id starts with _:"
            )
          }
        case "@type" if value.asText() != "Template" =>
          throw MetadataError(
            s"transformations[$index].@type  @type of transformation is not 'Template'"
          )

        case "url" =>
        case "titles" =>
        case _ =>
          val (_, w, newType) = checkProperty(property, value, baseUrl, lang)
          if (newType != PropertyType.Transformation || !w.isEmpty) {
            transformationsMainObject.remove(property)
            if (newType != PropertyType.Transformation)
              warnings = warnings :+ "invalid_property"
            warnings = Array.concat(warnings, w)
          }
      }
    }
    transformationsToReturn.add(transformationsMainObject)
    warnings
  }

  def checkProperty(
                     property: String,
                     value: JsonNode,
                     baseUrl: String,
                     lang: String
                   ): Either[MetadataError, (JsonNode, Array[String], PropertyType.Value)] = {
    if (Properties.contains(property)) {
      Properties(property)(value, baseUrl, lang)
    } else if (
      prefixedPropertyPattern
        .matches(property) && NameSpaces.values.contains(property.split(":")(0))
    ) {
      parseCommonPropertyValue(value, baseUrl, lang)
        .map({
          case (newValue, warnings) =>
            (newValue, warnings, PropertyType.Annotation)
        })
    } else {
      // property name must be an absolute URI
      asUri(property)
        .map(_ => {
          try {
            parseCommonPropertyValue(value, baseUrl, lang)
              .map({
                case (newValue, warnings) =>
                  (newValue, warnings, PropertyType.Annotation)
              })
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

  private def asUri(property: String): Option[URI] =
    Option(new URI(property))
      .filter(!_.getScheme.isEmpty)

  private def parseBooleanProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => Either[MetadataError, (
    JsonNode,
      Array[String],
      PropertyType.Value
    )] =
    (value, _, _) => {
      if (value.isBoolean) {
        Right((value, Array[String](), csvwPropertyType))
      } else {
        Right((
          BooleanNode.getFalse,
          Array[String](PropertyChecker.invalidValueWarning),
          csvwPropertyType
        ))
      }
    }

  private def parseCommonPropertyValue(
                                        commonPropertyValueNode: JsonNode,
                                        baseUrl: String,
                                        defaultLang: String
                                      ): Either[MetadataError, (JsonNode, StringWarnings)] = {
    commonPropertyValueNode match {
      case o: ObjectNode => parseCommonPropertyObject(o, baseUrl, defaultLang)
      case _: TextNode =>
        defaultLang match {
          case "und" => Right((commonPropertyValueNode, Array()))
          case _ =>
            val objectNodeToReturn = JsonNodeFactory.instance.objectNode()
            objectNodeToReturn.set("@value", commonPropertyValueNode)
            objectNodeToReturn.set("@language", new TextNode(defaultLang))
            Right((objectNodeToReturn, Array()))
        }
      case a: ArrayNode =>
        a.elements().asScala
          .map(elementNode =>
            parseCommonPropertyValue(elementNode, baseUrl, defaultLang)
          )
          .toArrayNodeAndStringWarnings
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected input of type ${commonPropertyValueNode.getClass}"
        )
    }
  }

  private def parseCommonPropertyObject(
                                         objectNode: ObjectNode,
                                         baseUrl: String,
                                         defaultLang: String
                                       ): Either[MetadataError, (ObjectNode, StringWarnings)] = {
    objectNode.fields().asScala
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
          case (valueNode, warnings) => (propertyName, Some(valueNode), warnings)
        })
      })
      .toObjectNodeAndStringWarnings
  }

  private def parseCommonPropertyObjectId(
                                           baseUrl: String,
                                           v: JsonNode
                                         ): Either[MetadataError, JsonNode] = {
    if (baseUrl.isBlank) {
      Right(v)
    } else {
      val textValue = v.asText()
      if (PropertyChecker.startsWithUnderscore.matches(textValue)) {
        Left(
          MetadataError(
            s"@id must not start with '_:'  -  $textValue"
          )
        )
      }
      try {
        val newValue = new URL(new URL(baseUrl), textValue)
        Right(new TextNode(newValue.toString))
      } catch {
        case _: Exception =>
          Left(
            MetadataError(
              s"common property has invalid @id ($textValue)"
            )
          )
      }
    }
  }

  private def stringProperty(
                              csvwPropertyType: PropertyType.Value
                            ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    if (value.isTextual) {
      (value, Array[String](), csvwPropertyType)
    } else {
      (
        new TextNode(""),
        Array[String](PropertyChecker.invalidValueWarning),
        csvwPropertyType
      )
    }
  }
  }

  private def numericProperty(
                               csvwPropertyType: PropertyType.Value
                             ): (JsonNode, String, String) => (
    JsonNode,
      Array[String],
      PropertyType.Value
    ) = { (value, _, _) => {
    if (value.isInt && value.asInt >= 0) {
      (value, Array[String](), csvwPropertyType)
    } else if (value.isInt && value.asInt < 0) {
      (
        NullNode.getInstance(),
        Array[String](PropertyChecker.invalidValueWarning),
        csvwPropertyType
      )
    } else {
      (
        NullNode.getInstance(),
        Array[String](PropertyChecker.invalidValueWarning),
        csvwPropertyType
      )
    }
  }
  }

  private def parseNotesProperty(
                             csvwPropertyType: PropertyType.Value
                           ): (JsonNode, String, String) => Either[
    MetadataError,
    (
      JsonNode,
        Array[String],
        PropertyType.Value
      )
  ] = {
    def parseNotesPropertyInternal(
                               value: JsonNode,
                               baseUrl: String,
                               lang: String
                             ): Either[MetadataError, (JsonNode, Array[String], PropertyType.Value)] = {
      value match {
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .map(element => parseCommonPropertyValue(element, baseUrl, lang))
            .toArrayNodeAndStringWarnings
            .map({
              case (arrayNode, warnings) =>
                (arrayNode, warnings, csvwPropertyType)
            })
        case _ =>
          Right(
            (
              JsonNodeFactory.instance.arrayNode(),
              Array[String](PropertyChecker.invalidValueWarning),
              csvwPropertyType
            )
          )
      }
    }

    parseNotesPropertyInternal
  }

  private def processCommonPropertyObjectValue(
                                                value: ObjectNode
                                              ): Either[MetadataError, JsonNode] = {
    if (
      (!value.path("@type").isMissingNode) && (!value
        .path("@language")
        .isMissingNode)
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
                                                 valueCopy: ObjectNode,
                                                 v: JsonNode
                                               ): Either[MetadataError, JsonNode] = {
    if (valueCopy.path("@value").isMissingNode) {
      Left(MetadataError("common property with @language lacks a @value"))
    } else {
      val language = v.asText()
      if (language.isEmpty || !Bcp47Language.r.matches(language)) {
        Left(
          MetadataError(
            s"common property has invalid @language (${language})"
          )
        )
      } else {
        Right(v)
      }
    }
  }

  @tailrec
  private def parseCommonPropertyObjectType(
                                             objectNode: ObjectNode,
                                             p: String,
                                             v: JsonNode
                                           ): Either[MetadataError, (JsonNode, StringWarnings)] = {
    val valueNode = objectNode.path("@value")
    v match {
      case s: TextNode =>
        val dataType = s.asText()

        val isCsvWDataType =
          valueNode.isMissingNode && CsvWDataTypes.contains(dataType)
        val isXsdDataType =
          !valueNode.isMissingNode && XsdDataTypes.types.contains(dataType)
        if (isCsvWDataType || isXsdDataType) {
          Right(s, Array.empty)
        } else {
          val arr: ArrayNode = JsonNodeFactory.instance.arrayNode()
          arr.add(s)
          parseCommonPropertyObjectType(objectNode, p, arr)
        }
      case a: ArrayNode =>
        a.elements().asScala
          .map(typeElement => {
            val dataType = typeElement.asText()
            if (
              prefixedPropertyPattern.matches(dataType) && NameSpaces.values
                .contains(dataType.split(":")(0))
            ) {
              Right(a, Array[String]())
            } else {
              // typeElement Must be an absolute URI
              try {
                asUri(dataType)
                  .map(_ => Right(a, Array[String]()))
                  .getOrElse(
                    Left(
                      MetadataError(
                        s"common property has invalid @type (${dataType})"
                      )
                    )
                  )
              } catch {
                case _: Exception =>
                  Left(
                    MetadataError(
                      s"common property has invalid @type (${dataType})"
                    )
                  )
              }
            }
          })
          .toArrayNodeAndStringWarnings
    }
  }

  implicit class MetadataErrorsOrArrayElements(iterator: Iterator[Either[MetadataError, (JsonNode, StringWarnings)]]) {
    def toArrayNodeAndStringWarnings: Either[MetadataError, (ArrayNode, StringWarnings)] = {
      iterator.foldLeft[Either[MetadataError, (ArrayNode, StringWarnings)]](
        Right(JsonNodeFactory.instance.arrayNode(), Array())
      )({
        case (err@Left(_), _) => err
        case (_, Left(newError)) => Left(newError)
        case (
          Right((parsedArrayNode, warnings)),
          Right((parsedElementNode, newWarnings))
          ) =>
          Right(
            (
              parsedArrayNode.deepCopy().add(parsedElementNode),
              warnings ++ newWarnings
            )
          )
      })

    }

  }

  implicit class MetadataErrorsOrObjectProperties(iterator: Iterator[Either[MetadataError, (String, Option[JsonNode], StringWarnings)]]) {
    def toObjectNodeAndStringWarnings: Either[MetadataError, (ObjectNode, StringWarnings)] = {
      val accumulator: Either[MetadataError, (ObjectNode, StringWarnings)] = Right(JsonNodeFactory.instance.objectNode(), Array())
      iterator.foldLeft(accumulator)({
        case (err@Left(_), _) => err
        case (_, Left(newError)) => Left(newError)
        case (
          Right((objectNode, warnings)),
          Right((key, valueNode, newWarnings))
          ) =>
          valueNode match {
            case Some(newValue) =>
              Right(
                objectNode.deepCopy().set(key, newValue),
                warnings ++ newWarnings
              )
            case None =>
              // Value is 'deleted' (not added to modified node).
              Right(
                objectNode,
                warnings ++ newWarnings
              )
          }
      })
    }
  }

}


