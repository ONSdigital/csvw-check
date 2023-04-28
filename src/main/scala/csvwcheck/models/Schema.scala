package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.errors.MetadataError

import java.io.{File, PrintWriter, StringWriter}
import java.net.URI

object Schema {
  def loadMetadataAndValidate(
      schemaUri: URI
  ): Either[String, (TableGroup, WarningsAndErrors)] = {
    try {
      val jsonNode = if (schemaUri.getScheme == "file") {
        objectMapper.readTree(new File(schemaUri))
      } else {
        objectMapper.readTree(schemaUri.toURL)
      }
      Right(
        Schema.fromCsvwMetadata(
          schemaUri.toString,
          jsonNode.asInstanceOf[ObjectNode]
        )
      )
    } catch {
      case metadataError: MetadataError => Left(metadataError.getMessage)
      case e: Throwable =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        Left(sw.toString)
    }
  }

  def fromCsvwMetadata(
      uri: String,
      json: ObjectNode
  ): Either[MetadataError, ParsedResult[TableGroup]] = {
    TableGroup.fromJson(json, uri)
  }

}

case class Schema(
    uri: String,
    title: JsonNode,
    description: JsonNode
) {}
