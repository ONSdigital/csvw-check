package csvwcheck.standardisers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.standardisers.Constants.undefinedLanguage
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedArrayElements, StringWarnings}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object TableGroup {

  private val parsers: Map[String, JsonNodeParser] = Map(
    "@type" -> Utils.parseRequiredType(PropertyType.Common, "TableGroup"),
    "@context" -> Context.parseContext(PropertyType.Common),
    "tables" -> parseTables(PropertyType.TableGroup)
  )

  def standardiseTableGroup(tableGroupNode: ObjectNode, baseUrl: String, lang: String = undefinedLanguage): ParseResult[(ObjectNode, Array[WarningWithCsvContext])] =
    tableGroupNode match {
      case tableGroupNode: ObjectNode =>
        val standardisedTableGroupStructure = standardiseSingleTableToTableGroupStructure(tableGroupNode)
        Utils.parseObjectNode(standardisedTableGroupStructure, parsers, baseUrl, lang)
          .map({
            case (parsedTableGroupNode, stringWarnings) =>
              (
                parsedTableGroupNode,
                // todo: mapping of error please.
                stringWarnings.map(w => WarningWithCsvContext(
                  "TODO",
                  "",
                  "",
                  "",
                  s"TODO: $w",
                  ""
                )
              )
            )
          })
      case tableGroupNode => Left(MetadataError(s"Unexpected table group value ${tableGroupNode.toPrettyString}"))
    }

  private def standardiseSingleTableToTableGroupStructure(tableGroupNode: ObjectNode): ObjectNode = {
    tableGroupNode.getMaybeNode("tables")
      .map(_ => tableGroupNode)
      .orElse(
        tableGroupNode.getMaybeNode("url").map(_ => {
          val newTableGroupNode = JsonNodeFactory.instance.objectNode()
          val tables = JsonNodeFactory.instance.arrayNode()
          val newTableNode = tableGroupNode.deepCopy()

          tables.add(newTableNode)
          newTableGroupNode.set("tables", tables)

          // Bring the context node along for the ride
          tableGroupNode
            .getMaybeNode("@context")
            .foreach(contextNode => {
              newTableGroupNode.set("@context", contextNode)
              newTableNode.remove("@context")
            })
          newTableGroupNode
        })
      )
      .getOrElse(tableGroupNode)
  }


  private def parseTables(propertyType: PropertyType.Value): JsonNodeParser = (tablesNode, baseUrl, lang) => tablesNode match {
    case tablesArrayNode: ArrayNode if tablesArrayNode.isEmpty() =>
      Left(MetadataError("Empty tables property"))
    case tablesArrayNode: ArrayNode =>
      tablesArrayNode.elements().asScala
        .map(tableNode => {
          Table.parseTable(PropertyType.TableGroup)(tableNode, baseUrl, lang)
            .map({
              case (tableNode, stringWarnings, _) => (Some(tableNode), stringWarnings)
            })
        })
        .toArrayNodeAndStringWarnings
        .map(_ :+ propertyType)
    case tablesNode => Left(MetadataError(s"Unexpected tables value: ${tablesNode.toPrettyString}"))
  }

  def parseJsonProperty(
                         property: String,
                         value: JsonNode,
                         baseUrl: String,
                         lang: String
                       ): ParseResult[(JsonNode, StringWarnings, PropertyType.Value)] = Utils.parseJsonProperty(parsers, property, value, baseUrl, lang)
}
