package csvwcheck.propertyparser

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult

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
}
