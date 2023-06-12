package csvwcheck.normalisation

import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{CascadeToOtherFilesError, GeneralCsvwLoadError}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Context.getBaseUrlAndLanguageFromContext
import csvwcheck.normalisation.Utils.{MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, NormalisationContext, Normaliser, invalidValueWarning}
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import shapeless.syntax.std.tuple.productTupleOps
import sttp.client3.basicRequest
import sttp.model.Uri

import java.io.{File, FileNotFoundException}
import java.net.URI
import scala.jdk.CollectionConverters.IteratorHasAsScala

object TableSchema {
  private val normalisers: Map[String, Normaliser] = Map(
    // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#h-schemas
    "@context" -> Context.normaliseContext(PropertyType.Context),
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Schema"),
    // Schema Properties
    "columns" -> normaliseColumnsProperty(PropertyType.Schema),
    "foreignKeys" -> ForeignKey.normaliseForeignKeysProperty(PropertyType.Schema),
    "primaryKey" -> Utils.normaliseColumnReferenceProperty(PropertyType.Schema),
    "rowTitles" -> Utils.normaliseColumnReferenceProperty(PropertyType.Schema),
  ) ++ InheritedProperties.normalisers ++ IdProperty.normaliser

  def normaliseTableSchema(
                            csvwPropertyType: PropertyType.Value
                          ): Normaliser = {
    def tableSchemaPropertyInternal(context: NormalisationContext[JsonNode]): ParseResult[(JsonNode, MetadataWarnings, PropertyType.Value)] = {
      context.node match {
        case textNode: TextNode =>
          val schemaUrl = Utils.toAbsoluteUrl(textNode.asText(), context.baseUrl)

          fetchSchemaNode(context, schemaUrl)
            .flatMap(schemaNode => {
              val inDocumentSchemaContext = context.withNode(schemaNode)

              getBaseUrlAndLanguageFromContext(inDocumentSchemaContext)
                .flatMap({
                  case (schemaBaseUrl, schemaLanguage) =>
                    val externalSchemaContext = inDocumentSchemaContext.copy(baseUrl = schemaBaseUrl, language = schemaLanguage)
                    normaliseSchemaObjectNode(externalSchemaContext)
                      .map(_ :+ csvwPropertyType)
                })
            })
        case schemaNode: ObjectNode =>
          normaliseSchemaObjectNode(context.withNode(schemaNode))
            .map(_ :+ csvwPropertyType)
        case _ =>
          // If the supplied value of an object property is not a string or object (eg if it is an integer),
          // compliant applications MUST issue a warning and proceed as if the property had been specified as an
          // object with no properties.
          Right(
            (
              JsonNodeFactory.instance.objectNode(),
              Array(context.makeWarning(invalidValueWarning)),
              csvwPropertyType
            )
          )
      }
    }

    tableSchemaPropertyInternal
  }

  private def fetchSchemaNode(context: NormalisationContext[JsonNode], schemaUrl: String): ParseResult[ObjectNode] = {
    val schemaUri = new URI(schemaUrl)
    if (schemaUri.getScheme == "file") {
      try {
        Right(objectMapper.readTree(new File(schemaUri)).asInstanceOf[ObjectNode])
      } catch {
        case error: FileNotFoundException => Left(context.makeError(s"Could not fetch tableSchema at '$schemaUrl' - $error'", cause=error))
        case error: JsonMappingException => Left(context.makeError(s"Could not parse JSON of tableSchema '$schemaUrl' - $error", cause = error))
      }
    } else {
      val response = context.httpClient.send(basicRequest.get(Uri(schemaUrl)))
      response.body match {
        case Left(error) => Left(context.makeError(s"Could not fetch tableSchema at '$schemaUrl' - $error"))
        case Right(body) =>
          try {
            Right(objectMapper.readTree(body).asInstanceOf[ObjectNode])
          } catch {
            case error: JsonMappingException => Left(context.makeError(s"Could not parse JSON of tableSchema '$schemaUrl' - $error", cause = error))
          }
      }
    }
  }

  private def normaliseSchemaObjectNode(context: NormalisationContext[ObjectNode]): ParseResult[(ObjectNode, MetadataWarnings)] = {
    context.node.getKeysAndValues
      .map({ case (propertyName, value) =>
        val propertyContext = context.toChild(value, propertyName)
        Utils.normaliseJsonProperty(normalisers, propertyName, propertyContext)
          .map({
            case (parsedValue, warnings, _) => (propertyName, Some(parsedValue), warnings)
          })
      })
      .iterator
      .toObjectNodeAndWarnings
  }

  private def normaliseColumnsProperty(
                                        propertyType: PropertyType.Value
                                      ): Normaliser = context => context.node match {
    case columnsNode: ArrayNode =>
      columnsNode
        .elements()
        .asScala
        .zipWithIndex
        .map({
          case (columnNode: ObjectNode, index) =>
            val columnNodeContext = context.toChild(columnNode.asInstanceOf[JsonNode], index.toString)
            Column.normaliseColumn(propertyType)(columnNodeContext)
              .map({ case (parsedColumnNode, warnings, _) => (Some(parsedColumnNode), warnings) })
          case (columnNode, index) =>
            val columnNodeContext = context.toChild(columnNode, index.toString)
            // Any items within an array that are not valid objects of the type expected are ignored
            Right(
              (
                None,
                Array(columnNodeContext.makeWarning(s"Unexpected columns value: ${columnNode.toPrettyString}")),
              )
            )
        })
        .toArrayNodeAndWarnings
        .map(_ :+ propertyType)
    case columnsNode =>
      Left(context.makeError(s"Unexpected columns value: ${columnsNode.toPrettyString}"))
  }
}


