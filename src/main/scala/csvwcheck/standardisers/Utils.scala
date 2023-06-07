package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.{NameSpaces, XsdDataTypes}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Constants.{CsvWDataTypes, undefinedLanguage}
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import shapeless.syntax.std.tuple.productTupleOps

import java.net.{URI, URL}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

object Utils {
  type StringWarnings = Array[String]
  type JsonNodeParser =
    (JsonNode, String, String) => JsonNodeParseResult
  private type JsonNodeParseResult = Either[
    MetadataError,
    (JsonNode, StringWarnings, PropertyType.Value)
  ]

  type ObjectPropertyParseResult =
    ParseResult[(String, Option[JsonNode], StringWarnings)]
  type ArrayElementParseResult =
    ParseResult[(Option[JsonNode], StringWarnings)]

  val invalidValueWarning = "invalid_value"

  implicit class MetadataErrorsOrParsedArrayElements(iterator: Iterator[ArrayElementParseResult]) {
    def toArrayNodeAndStringWarnings: ParseResult[(ArrayNode, StringWarnings)] = {
      iterator.foldLeft[ParseResult[(ArrayNode, StringWarnings)]](
        Right(JsonNodeFactory.instance.arrayNode(), Array())
      )({
        case (err@Left(_), _) => err
        case (_, Left(newError)) => Left(newError)
        case (
          Right((parsedArrayNode, warnings)),
          Right((parsedElementNode, newWarnings))
          ) =>
          parsedElementNode match {
            case Some(arrayElement) =>
              Right(
                (
                  parsedArrayNode.deepCopy().add(arrayElement),
                  warnings ++ newWarnings
                )
              )
            case None => Right(parsedArrayNode, warnings ++ newWarnings)
          }
      })

    }

  }

  implicit class MetadataErrorsOrParsedObjectProperties(iterator: Iterator[ObjectPropertyParseResult]) {
    def toObjectNodeAndStringWarnings: ParseResult[(ObjectNode, StringWarnings)] = {
      val accumulator: ParseResult[(ObjectNode, StringWarnings)] =
        Right(JsonNodeFactory.instance.objectNode(), Array())
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

  def parseStringProperty(
                           csvwPropertyType: PropertyType.Value
                         ): JsonNodeParser = { (value, _, _) => {
    value match {
      case _: TextNode => Right((value, Array.empty, csvwPropertyType))
      case _ =>
        Right(
          new TextNode(""),
          Array(invalidValueWarning),
          csvwPropertyType
        )
    }
  }
  }

  def parseNodeAsText(
                       valueNode: JsonNode,
                       coerceToText: Boolean = false
                     ): ParseResult[String] =
    valueNode match {
      case textNode: TextNode => Right(textNode.asText)
      case node if coerceToText => Right(node.asText)
      case node =>
        Left(
          MetadataError(
            s"Unexpected value, expected string/text but got: ${node.toPrettyString}"
          )
        )
    }

  def parseNodeAsInt(valueNode: JsonNode): ParseResult[Int] =
    valueNode match {
      case numericNode: IntNode => Right(numericNode.asInt)
      case node =>
        Left(
          MetadataError(
            s"Unexpected value, expected integer but got: ${node.toPrettyString}"
          )
        )
    }

  def parseNodeAsBool(valueNode: JsonNode): ParseResult[Boolean] =
    valueNode match {
      case booleanNode: BooleanNode => Right(booleanNode.asBoolean())
      case node =>
        Left(
          MetadataError(
            s"Unexpected value, expected boolean but got: ${node.toPrettyString}"
          )
        )
    }

  def parseBooleanProperty(
                            csvwPropertyType: PropertyType.Value
                          ): JsonNodeParser =
    (value, _, _) => {
      if (value.isBoolean) {
        Right((value, Array[String](), csvwPropertyType))
      } else {
        Right(
          (
            BooleanNode.getFalse,
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
      }
    }

  def parseNonNegativeIntegerProperty(
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

  def parseJsonProperty(
                         propertyParsers: Map[String, JsonNodeParser],
                         propertyName: String,
                         value: JsonNode,
                         baseUrl: String,
                         lang: String
                     ): ParseResult[(JsonNode, StringWarnings, PropertyType.Value)] = {
  if (propertyParsers.contains(propertyName)) {
    propertyParsers(propertyName)(value, baseUrl, lang)
  } else if (
    RegExpressions.prefixedPropertyPattern
      .matches(propertyName) && NameSpaces.values.contains(propertyName.split(":")(0))
  ) {
    parseCommonPropertyValue(value, baseUrl, lang)
      .map(_ :+ PropertyType.Annotation)
  } else {
    // property name must be an absolute URI
    asUri(propertyName)
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

  def parseCommonPropertyValue(
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
            Utils.parseCommonPropertyValue(elementNode, baseUrl, defaultLang)
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

  def asUri(property: String): Option[URI] =
    Option(new URI(property))
      .filter(uri => uri.getScheme != null && uri.getScheme.nonEmpty)

  def parseCommonPropertyObject(
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
              Utils.parseCommonPropertyValue(propertyValueNode, baseUrl, defaultLang)
            }
        }).map({
          case (valueNode, warnings) =>
            (propertyName, Some(valueNode), warnings)
        })
      })
      .toObjectNodeAndStringWarnings
  }

  def parseCommonPropertyObjectId(
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

  @tailrec
   def parseCommonPropertyObjectType(
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
          Utils.parseCommonPropertyObjectType(objectNode, p, arr)
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
                Utils.asUri(dataType)
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

  def processCommonPropertyObjectValue(
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

  def parseCommonPropertyObjectLanguage(
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

  def parseUrlLinkProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (v, baseUrl, _) => {
    v match {
      case urlNode: TextNode =>
        val urlValue = urlNode.asText()
        if (RegExpressions.startsWithUnderscore.matches(urlValue)) {
          Left(MetadataError(s"'$urlValue' starts with _:"))
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

  def parseLanguageProperty(
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

  def parseColumnReferenceProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): JsonNodeParser = { (value, _, _) => {
    value match {
      case textNode: TextNode =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode().add(textNode),
            Array.empty,
            csvwPropertyType
          )
        )
      case arrayNode: ArrayNode =>
        arrayNode.elements()
          .asScala
          .map({
            case columnReference: TextNode => Right((Some(columnReference), Array[String]()))
            case columnReferenceNode => Left(
                MetadataError(
                  s"Unexpected columnReference '${columnReferenceNode.toPrettyString}'"
                )
              )
          })
          .toArrayNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Left(MetadataError(s"Unexpected column reference value $value"))
    }
  }
  }

  def parseDoNothing(propertyType: PropertyType.Value): JsonNodeParser =
    (value, _, _) => Right((value, Array[String](), propertyType))

  def parseRequiredType(propertyType: PropertyType.Value, requiredType: String): JsonNodeParser = (value, _, _) =>
    Utils.parseNodeAsText(value)
      .flatMap(declaredType =>
        if (declaredType == requiredType) {
          Right((value, Array.empty, propertyType))
        } else {
          Left(
            MetadataError(
              s"@type must be '{$requiredType}', found (${value.toPrettyString})"
            )
          )
        }
      )

  def parseObjectNode(objectNode: ObjectNode, parsers: Map[String, JsonNodeParser], baseUrl: String, lang: String): ParseResult[(ObjectNode, StringWarnings)] =
    objectNode.getKeysAndValues
      .map({
        case (propertyName, value) if parsers.contains(propertyName) =>
          parseJsonProperty(parsers, propertyName, value, baseUrl, lang)
            .map({
              case (jsonNode, stringWarnings, _) => (propertyName, Some(jsonNode), stringWarnings)
            })
        case (propertyName, _) => Left(MetadataError(s"Unexpected property '$propertyName' in ${objectNode.toPrettyString}"))
      })
      .iterator
      .toObjectNodeAndStringWarnings

  def asAbsoluteUrl(
                             csvwPropertyType: PropertyType.Value
                           ): JsonNodeParser = { (value, baseUrl, _) =>

    parseNodeAsText(value)
      .map(possiblyRelativeUrl => new TextNode(
          new URL(
            new URL(baseUrl),
            possiblyRelativeUrl
          )
          .toString
        )
      )
      .map((_, Array.empty, csvwPropertyType))
  }
}
