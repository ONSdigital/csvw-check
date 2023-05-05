package csvwcheck.models

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import csvwcheck.enums.TrimType
import csvwcheck.errors.MetadataError
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode

object Dialect {

  def fromJson(dialectNode: ObjectNode): ParseResult[Dialect] = {
    val encoding = dialectNode
      .getMaybeNode("encoding")
      .map(n => n.asText())
      .getOrElse("UTF-8")

    val quoteChar = dialectNode
      .getMaybeNode("quoteChar")
      .map(_.asText.charAt(0))
      .getOrElse('\"')

    val doubleQuote = dialectNode
      .getMaybeNode("doubleQuote")
      .forall(_.asBoolean())

    val commentPrefix = dialectNode
      .getMaybeNode("commentPrefix")
      .map(_.asText())
      .getOrElse("#")

    val header =
      dialectNode.getMaybeNode("header").forall(_.asBoolean())

    val headerRowCount =
      dialectNode.getMaybeNode("headerRowCount").map(_.asInt).getOrElse(1)

    val skipRows =
      dialectNode.getMaybeNode("skipRows").map(_.asInt()).getOrElse(0)

    val delimiter = dialectNode
      .getMaybeNode("delimiter")
      .map(_.asText().charAt(0))
      .getOrElse(',')

    val skipColumns =
      dialectNode.getMaybeNode("skipColumns").map(_.asInt()).getOrElse(0)

    val skipBlankRows = dialectNode
      .getMaybeNode("skipBlankRows")
      .exists(_.asBoolean())

    val skipInitialSpace = dialectNode
      .getMaybeNode("skipInitialSpace")
      .exists(_.asBoolean)
    val trim = dialectNode
      .getMaybeNode("trim")
      .map(n => TrimType.fromString(n.asText))
      .getOrElse(TrimType.False)

    val lineTerminatorsResult = dialectNode
      .getMaybeNode("lineTerminator")
      .map {
        case n: TextNode  => Right(Array(n.asText()))
        case n: ArrayNode => Right(n.iterator().asScalaArray.map(_.asText()))
        case n =>
          Left(MetadataError(s"Unexpected node type ${n.getClass.getName}"))
      }
      .getOrElse(Right(Array("\n", "\r\n")))

    lineTerminatorsResult.map(
      Dialect(
        encoding,
        _,
        quoteChar,
        doubleQuote,
        skipRows,
        commentPrefix,
        header,
        headerRowCount,
        delimiter,
        skipColumns,
        skipBlankRows,
        skipInitialSpace,
        trim
      )
    )
  }
}

case class Dialect(
    encoding: String = "UTF-8",
    lineTerminators: Array[String] = Array("\n", "\r\n"),
    quoteChar: Char = '\"',
    doubleQuote: Boolean = true,
    skipRows: Int = 0,
    commentPrefix: String = "#",
    header: Boolean = true,
    headerRowCount: Int = 1,
    delimiter: Char = ',',
    skipColumns: Int = 0,
    skipBlankRows: Boolean = false,
    skipInitialSpace: Boolean = false,
    trim: TrimType.Value = TrimType.True
)
