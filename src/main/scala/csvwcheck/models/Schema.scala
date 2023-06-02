package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.models.ParseResult.ParseResult

import java.io.{File, PrintWriter, StringWriter}
import java.net.URI

object Schema {
  def loadMetadataAndValidate(
      schemaUri: URI
  ): Either[String, WithWarningsAndErrors[TableGroup]] = {
    try {
      val jsonNode = if (schemaUri.getScheme == "file") {
        objectMapper.readTree(new File(schemaUri))
      } else {
        objectMapper.readTree(schemaUri.toURL)
      }

      Schema.fromCsvwMetadata(
        schemaUri.toString,
        jsonNode.asInstanceOf[ObjectNode]
      ) match {
        case Right(tableGroup)   => Right(tableGroup)
        case Left(metadataError) => Left(metadataError.getMessage)
      }
    } catch {
      case e: Throwable =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        Left(sw.toString)
    }
  }

  def fromCsvwMetadata(
      uri: String,
      json: ObjectNode
  ): ParseResult[WithWarningsAndErrors[TableGroup]] = {
    TableGroup.fromJson(json, uri)
  }

}

case class Schema(
    uri: String,
    title: JsonNode,
    description: JsonNode
) {}
