package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{MetadataError, MetadataWarning}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.Context.getBaseUrlAndLanguageFromContext
import csvwcheck.normalisation.Utils.{Normaliser, MetadataErrorsOrParsedArrayElements, MetadataErrorsOrParsedObjectProperties, MetadataWarnings, ObjectPropertyNormaliserResult, PropertyPath, invalidValueWarning}
import shapeless.syntax.std.tuple.productTupleOps

import java.net.URL
import scala.jdk.CollectionConverters.IteratorHasAsScala

object TableSchemaProperties {
  private val normalisers: Map[String, Normaliser] = Map(
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
    def tableSchemaPropertyInternal(
                                     value: JsonNode,
                                     inheritedBaseUrlStr: String,
                                     inheritedLanguage: String,
                                     propertyPath: PropertyPath
                                   ): ParseResult[(JsonNode, MetadataWarnings, PropertyType.Value)] = {
      val inheritedBaseUrl = new URL(inheritedBaseUrlStr)
      value match {
        case textNode: TextNode =>
          val schemaUrl = new URL(inheritedBaseUrl, textNode.asText())
          // todo: Need to use injected HTTP-boi to download the schema here.
          val schemaNode =
            objectMapper.readTree(schemaUrl).asInstanceOf[ObjectNode]

          getBaseUrlAndLanguageFromContext(schemaUrl.toString, inheritedLanguage, schemaNode)
            .map({ case (baseUrl, lang) => (new URL(baseUrl), lang) })
            .flatMap({
              case (tableSchemaBaseUrl, newLang) =>
                schemaNode.fields.asScala
                  .map(fieldAndValue =>
                    normaliseTableSchemaObjectProperty(
                      fieldAndValue.getKey,
                      fieldAndValue.getValue,
                      // N.B. The baseUrl of the child part of the document is now the URL where the table schema is
                      // defined.
                      tableSchemaBaseUrl,
                      newLang,
                      propertyPath :+ fieldAndValue.getKey
                    )
                  )
                  .toObjectNodeAndWarnings
                  .map(_ :+ csvwPropertyType)
            })
        case schemaNode: ObjectNode =>
          schemaNode.fields.asScala
            .map(fieldAndValue =>
              normaliseTableSchemaObjectProperty(
                fieldAndValue.getKey,
                fieldAndValue.getValue,
                inheritedBaseUrl,
                inheritedLanguage,
                propertyPath :+ fieldAndValue.getKey
              )
            )
            .toObjectNodeAndWarnings
            .map(_ :+ csvwPropertyType)
        case _ =>
          // If the supplied value of an object property is not a string or object (eg if it is an integer),
          // compliant applications MUST issue a warning and proceed as if the property had been specified as an
          // object with no properties.
          Right(
            (
              JsonNodeFactory.instance.objectNode(),
              Array(MetadataWarning(propertyPath, invalidValueWarning)),
              csvwPropertyType
            )
          )
      }
    }

    tableSchemaPropertyInternal
  }



  private def normaliseTableSchemaObjectProperty(
                                              propertyName: String,
                                              value: JsonNode,
                                              tableSchemaBaseUrl: URL,
                                              language: String,
                                              propertyPath: PropertyPath
                                            ): ObjectPropertyNormaliserResult = {
      Utils.normaliseJsonProperty(normalisers, propertyPath, propertyName, value, tableSchemaBaseUrl.toString, language)
            .map({
              case (parsedValue, warnings, _) => (propertyName, Some(parsedValue), warnings)
            })
  }

  private def normaliseColumnsProperty(
                            propertyType: PropertyType.Value
                          ): Normaliser = (columnsNode, baseUrl, lang, propertyPath) => columnsNode match {
    case columnsNode: ArrayNode =>
      columnsNode
        .elements()
        .asScala
        .zipWithIndex
        .map({
          case (columnNode: ObjectNode, index) => Column.normaliseColumn(propertyType)(columnNode, baseUrl, lang, propertyPath :+ index.toString)
            .map({ case (parsedColumnNode, warnings, _) => (Some(parsedColumnNode), warnings)})
          case (columnNode, index) =>
            // Any items within an array that are not valid objects of the type expected are ignored
          Right(
              (
                None,
                Array(MetadataWarning(propertyPath :+ index.toString, s"Unexpected columns value: ${columnNode.toPrettyString}")),
              )
            )
        })
        .toArrayNodeAndWarnings
        .map(_ :+ propertyType)
    case columnsNode =>
      Left(MetadataError(s"Unexpected columns value: ${columnsNode.toPrettyString}", propertyPath))
  }
}
