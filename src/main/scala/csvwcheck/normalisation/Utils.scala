package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.{NameSpaces, XsdDataTypes}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Constants.{CsvWDataTypes, undefinedLanguage}
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import shapeless.syntax.std.tuple.productTupleOps

import java.net.{URI, URL}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

object Utils {
  type MetadataWarnings = Array[MetadataWarning]
  type PropertyPath = Array[String]

  /**
    * Context when normalising a particular node wiithin the JSON document
    * @param node The node being normalised
    * @param baseUrl The baseUrl for the current JSON-LD document
    * @param language The language for the current JSON-LD document
    * @param propertyPath The path from the root node to this particular node
    */
  case class NormContext[T <: JsonNode](node: T, baseUrl: String, language: String, propertyPath: PropertyPath) {
    def withNode[TSpecial <: JsonNode](specialisedNode: TSpecial): NormContext[TSpecial] =
      this.copy(node = specialisedNode)

    def withNodeAs[TSpecial <: JsonNode](): NormContext[TSpecial] =
      this.copy(node = this.node.asInstanceOf[TSpecial])

    def toChild[TChild <: JsonNode](childNode: TChild, pathAddition: String): NormContext[TChild] =
      this.copy(node = childNode, propertyPath = this.propertyPath :+ pathAddition)

    def makeWarning(message: String): MetadataWarning = MetadataWarning(path = this.propertyPath, message = message)

    def makeError(message: String): MetadataError = MetadataError(message = message, propertyPath = this.propertyPath)
  }

  type Normaliser = NormContext[JsonNode] => NormaliserResult

  private type NormaliserResult = Either[
    MetadataError,
    (JsonNode, MetadataWarnings, PropertyType.Value)
  ]

  type ObjectPropertyNormaliserResult =
    ParseResult[(String, Option[JsonNode], MetadataWarnings)]
  type ArrayElementNormaliserResult =
    ParseResult[(Option[JsonNode], MetadataWarnings)]

  val noWarnings = Array[MetadataWarning]()
  val invalidValueWarning = "invalid_value"

  implicit class MetadataErrorsOrParsedArrayElements(iterator: Iterator[ArrayElementNormaliserResult]) {
    def toArrayNodeAndWarnings: ParseResult[(ArrayNode, MetadataWarnings)] = {
      iterator.foldLeft[ParseResult[(ArrayNode, MetadataWarnings)]](
        Right(JsonNodeFactory.instance.arrayNode(), Array())
      )({
        case (err@Left(_), _) => err
        case (_, Left(newError)) => Left(newError)
        case (
          Right((parsedArrayNode, warnings)),
          Right((parsedElementNode, newWarnings))
          ) =>
          parsedElementNode match {
            case Some(arrayElement) if arrayElement.isEmpty =>
              Right(
                (
                  parsedArrayNode,
                  warnings ++ newWarnings
                )
              )
            case Some(arrayElement) =>
              Right(
                (
                  parsedArrayNode.add(arrayElement),
                  warnings ++ newWarnings
                )
              )
            case None => Right(parsedArrayNode, warnings ++ newWarnings)
          }
      })

    }

  }

  implicit class MetadataErrorsOrParsedObjectProperties(iterator: Iterator[ObjectPropertyNormaliserResult]) {
    def toObjectNodeAndWarnings: ParseResult[(ObjectNode, MetadataWarnings)] = {
      val accumulator: ParseResult[(ObjectNode, MetadataWarnings)] =
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

  def normaliseStringProperty(
                           csvwPropertyType: PropertyType.Value
                         ): Normaliser = { context =>
    context.node match {
      case textNode: TextNode => Right((textNode, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          new TextNode(""),
          Array(context.makeWarning(invalidValueWarning)),
          csvwPropertyType
        )
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

  def normaliseBooleanProperty(
                            csvwPropertyType: PropertyType.Value
                          ): Normaliser =
    (value, _, _, path) => {
      if (value.isBoolean) {
        Right((value, noWarnings, csvwPropertyType))
      } else {
        Right(
          (
            BooleanNode.getFalse,
            Array(MetadataWarning(path, invalidValueWarning)),
            csvwPropertyType
          )
        )
      }
    }

  def normaliseNonNegativeIntegerProperty(
                                       csvwPropertyType: PropertyType.Value
                                     ): Normaliser = { (value, _, _, path) =>
    value match {
      case value: IntNode if value.asInt() >= 0 =>
        Right((value, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.getInstance(),
            Array(MetadataWarning(path, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }

  def normaliseJsonProperty[T <: JsonNode](
                             normalisers: Map[String, Normaliser],
                             propertyName: String,
                             propertyContext: NormContext[T]
                     ): ParseResult[(JsonNode, MetadataWarnings, PropertyType.Value)] = {
  if (normalisers.contains(propertyName)) {
    normalisers(propertyName)(propertyContext)
  } else if (
    RegExpressions.prefixedPropertyPattern
      .matches(propertyName) && NameSpaces.values.contains(propertyName.split(":")(0))
  ) {
    normaliseCommonPropertyValue(propertyContext)
      .map(_ :+ PropertyType.Annotation)
  } else {
    // property name must be an absolute URI
    asUri(propertyName)
      .map(_ => {
        try {
          normaliseCommonPropertyValue(propertyContext)
            .map(_ :+ PropertyType.Annotation)
        } catch {
          case e: Exception =>
            Right(
              (
                value,
                Array(MetadataWarning(pathToPropertyValue, s"invalid_property ${e.getMessage}")),
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
            Array(MetadataWarning(pathToPropertyValue, "invalid_property")),
            PropertyType.Undefined
          )
        )
      )
  }
}

  def normaliseCommonPropertyValue(context: NormContext[JsonNode]): ParseResult[(JsonNode, MetadataWarnings)] = {
    context.node match {
      case o: ObjectNode => normaliseCommonPropertyObject(context.withNode(o))
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
          .zipWithIndex
          .map({ case (elementNode, index) =>
            val elementPropertyPath = propertyPath :+ index.toString
            normaliseCommonPropertyValue(elementNode, baseUrl, defaultLang, elementPropertyPath)
              .map({
                case (parsedElementNode, warnings) =>
                  (Some(parsedElementNode), warnings)
              })
          })
          .toArrayNodeAndWarnings
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

  def normaliseCommonPropertyObject(context: NormContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] = {
    context.node
      .getKeysAndValues
      .map({ case (propertyName, valueNode) =>
        (propertyName match {
          case "@context" | "@list" | "@set" =>
            Left(
              MetadataError(
                s"$propertyName: common property has $propertyName property"
              )
            )
          case "@type" =>
            normaliseCommonPropertyObjectType(
              context,
              propertyName,
              valueNode
            )
          case "@id" =>
            normaliseCommonPropertyObjectId(baseUrl, propertyValueNode)
              .map(v => (v, noWarnings))
          case "@value" =>
            processCommonPropertyObjectValue(objectNode)
              .map(v => (v, noWarnings))
          case "@language" =>
            normaliseCommonPropertyObjectLanguage(objectNode, propertyValueNode)
              .map(v => (v, noWarnings))
          case _ =>
            if (propertyName(0).equals('@')) {
              Left(
                MetadataError(
                  s"common property has property other than @id, @type, @value or @language beginning with @ ($propertyName)"
                )
              )
            } else {
              Utils.normaliseCommonPropertyValue(propertyValueNode, baseUrl, defaultLang, childPropertyPath)
            }
        }).map({
          case (valueNode, warnings) =>
            (propertyName, Some(valueNode), warnings)
        })
      })
      .toObjectNodeAndWarnings
  }

  def normaliseCommonPropertyObjectId(
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
  def normaliseCommonPropertyObjectType(context: NormContext[ObjectNode], typePropertyName: String, typeNode: JsonNode): ParseResult[(JsonNode, MetadataWarnings)] = {
    val valueNode = context.node.getMaybeNode("@value")
    typeNode match {
      case s: TextNode =>
        val dataType = s.asText()

        val isCsvWDataType =
          valueNode.isEmpty && CsvWDataTypes.contains(dataType)
        val isXsdDataType =
          valueNode.isDefined && XsdDataTypes.types.contains(dataType)
        if (isCsvWDataType || isXsdDataType) {
          Right((s, noWarnings))
        } else {
          val arr: ArrayNode = JsonNodeFactory.instance.arrayNode()
          arr.add(s)
          normaliseCommonPropertyObjectType(context, typePropertyName, arr)
        }
      case a: ArrayNode =>
        a.elements()
          .asScala
          .zipWithIndex
          .map({ case (typeElement, index) =>
            val elementPropertyPath = context.propertyPath :+ index.toString
            val dataType = typeElement.asText()
            if (
              RegExpressions.prefixedPropertyPattern.matches(dataType) && NameSpaces.values
                .contains(dataType.split(":")(0))
            ) {
              Right(Some(a), noWarnings)
            } else {
              // typeElement Must be an absolute URI
              try {
                Utils.asUri(dataType)
                  .map(_ => Right(Some(a), noWarnings))
                  .getOrElse(
                    Left(
                      MetadataError(
                        s"common property has invalid @type ($dataType)",
                        elementPropertyPath
                      )
                    )
                  )
              } catch {
                case _: Exception =>
                  Left(
                    MetadataError(
                      s"common property has invalid @type ($dataType)",
                      elementPropertyPath
                    )
                  )
              }
            }
          })
          .toArrayNodeAndWarnings
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

  def normaliseCommonPropertyObjectLanguage(
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

  def normaliseUrlLinkProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): Normaliser = { (v, baseUrl, _, propertyPath) => {
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
            (new TextNode(baseUrlCopy), noWarnings, csvwPropertyType)
          )
        }
      case _ =>
        // If the supplied value of a link property is not a string (e.g. if it is an integer), compliant applications
        // MUST issue a warning and proceed as if the property had been supplied with an empty string.
        Right(
          (
            new TextNode(""),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  def normaliseLanguageProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): Normaliser = { (value, _, _, propertyPath) => {
    value match {
      case s: TextNode
        if RegExpressions.Bcp47LanguagetagRegExp.matches(s.asText) =>
        Right((s, noWarnings, csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode(""),
            Array(MetadataWarning(propertyPath, invalidValueWarning)),
            csvwPropertyType
          )
        )
    }
  }
  }

  def normaliseColumnReferenceProperty(
                                    csvwPropertyType: PropertyType.Value
                                  ): Normaliser = { (value, _, _, _) => {
    value match {
      case textNode: TextNode =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode().add(textNode),
            noWarnings,
            csvwPropertyType
          )
        )
      case arrayNode: ArrayNode =>
        arrayNode.elements()
          .asScala
          .map({
            case columnReference: TextNode => Right((Some(columnReference), noWarnings))
            case columnReferenceNode => Left(
                MetadataError(
                  s"Unexpected columnReference '${columnReferenceNode.toPrettyString}'"
                )
              )
          })
          .toArrayNodeAndWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Left(MetadataError(s"Unexpected column reference value $value"))
    }
  }
  }

  def normaliseDoNothing(propertyType: PropertyType.Value): Normaliser =
    (value, _, _, _) => Right((value, noWarnings, propertyType))

  def normaliseRequiredType(propertyType: PropertyType.Value, requiredType: String): Normaliser = (value, _, _, _) =>
    Utils.parseNodeAsText(value)
      .flatMap(declaredType =>
        if (declaredType == requiredType) {
          Right((value, noWarnings, propertyType))
        } else {
          Left(
            MetadataError(
              s"@type must be '{$requiredType}', found (${value.toPrettyString})"
            )
          )
        }
      )

  def normaliseObjectNode(normalisers: Map[String, Normaliser], objectContext: NormContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] =
    objectNode.getKeysAndValues
      .map({
        case (propertyName, value) =>
          normaliseJsonProperty(normalisers, localPropertyPath, propertyName, value, baseUrl, lang)
            .map({
              case (jsonNode, warnings, _) => (propertyName, Some(jsonNode), warnings)
            })
      })
      .iterator
      .toObjectNodeAndWarnings

  def asAbsoluteUrl(
                             csvwPropertyType: PropertyType.Value
                           ): Normaliser = { (value, baseUrl, _, _) =>

    parseNodeAsText(value)
      .map(possiblyRelativeUrl => new TextNode(
          new URL(
            new URL(baseUrl),
            possiblyRelativeUrl
          )
          .toString
        )
      )
      .map((_, noWarnings, csvwPropertyType))
  }
}
