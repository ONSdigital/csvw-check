package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.PropertyChecker
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import org.apache.commons.csv.CSVRecord
import shapeless.syntax.std.tuple.productTupleOps

import java.net.URL
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object TableGroup {
  val csvwContextUri = "http://www.w3.org/ns/csvw"
  val validProperties: Array[String] = Array[String]("tables", "notes", "@type")
  val containsWhitespaces: Regex = ".*\\s.*".r

  def fromJson(
      tableGroupNodeIn: ObjectNode,
      baseUri: String
  ): ParseResult[WithWarningsAndErrors[TableGroup]] = {
    val baseUrl = baseUri.trim
    if (containsWhitespaces.matches(baseUrl)) {
      // todo: Shouldn't this be a warning?
      println(
        "Warning: The path/url has whitespaces in it, please ensure its correctness. Proceeding with received " +
          "path/url .."
      )
    }
    val tableGroupNode = restructureIfNodeIsSingleTable(tableGroupNodeIn)

    parseTableGroupType(tableGroupNode)
      .flatMap(_ => processContextGetBaseUrlLang(tableGroupNodeIn, baseUrl, "und"))
      .flatMap({
        case (baseUrl, lang, warnings) =>
          partitionTableGroupProperties(
            tableGroupNode,
            baseUrl,
            lang
          ).map(props => (baseUrl, lang, props.copy(warnings = props.warnings ++ warnings)))
      })
      .flatMap({
        case all@(baseUrl, lang, PartitionedTableGroupProperties(_, common, inherited, _)) =>
          parseTables(
            tableGroupNode,
            baseUrl,
            lang,
            common,
            inherited
          ).map(all :+ _)
      })
      .flatMap({
        case (baseUrl, lang, parsedProperties, tablesWithWarningsAndErrors) =>
          parseForeignKeysLinkToReferencedTables(baseUrl, tablesWithWarningsAndErrors.component)
            .map(tables => (baseUrl, lang, parsedProperties, tablesWithWarningsAndErrors.copy(component=tables)))
      })
      .map({
        case (baseUrl, _, PartitionedTableGroupProperties(annotations, common, _, warnings), tablesWithWarningsAndErrors) =>
          val tableGroup = TableGroup(
            baseUrl,
            getId(common),
            tablesWithWarningsAndErrors.component,
            common.get("notes"),
            annotations
          )

          WithWarningsAndErrors(
            tableGroup,
            WarningsAndErrors(warnings = warnings ++ tablesWithWarningsAndErrors.warningsAndErrors.warnings, errors = tablesWithWarningsAndErrors.warningsAndErrors.errors)
          )
      })
  }

  private def restructureIfNodeIsSingleTable(
      tableGroupNode: ObjectNode
  ): ObjectNode = {
    if (tableGroupNode.getMaybeNode("tables").isEmpty) {
      if (tableGroupNode.getMaybeNode("url").isDefined) {
        val newTableGroup = JsonNodeFactory.instance.objectNode()
        val tables = JsonNodeFactory.instance.arrayNode()
        tables.insert(0, tableGroupNode)
        newTableGroup.set("tables", tables)
        return newTableGroup
      }
    }
    tableGroupNode
  }

  def processContextGetBaseUrlLang(
      rootNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[(String, String, Array[WarningWithCsvContext])] = {
    (rootNode.get("@context") match {
      case a: ArrayNode => validateContextArrayNode(a, baseUrl, lang)
      case s: TextNode if s.asText == csvwContextUri =>
        Right((baseUrl, lang, Array[WarningWithCsvContext]()))
      case _ => Left(MetadataError("Invalid Context"))
    }).map(results => {
      rootNode.remove("@context")
      results
    })
  }

  // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#top-level-properties
  def validateContextArrayNode(
      context: ArrayNode,
      baseUrl: String,
      lang: String
  ): ParseResult[(String, String, Array[WarningWithCsvContext])] = {
    def validateFirstItemInContext(
        firstItem: JsonNode
    ): ParseResult[Unit] = {
      firstItem match {
        case s: TextNode if s.asText == csvwContextUri => Right()
        case _ =>
          Left(
            MetadataError(
              s"First item in @context must be string $csvwContextUri "
            )
          )
      }
    }

    context.elements().asScalaArray match {
      case Array(firstItem, secondItem) =>
        // if @context contains 2 elements, the first element will be the namespace for csvw - http://www.w3.org/ns/csvw
        // The second element can be @language or @base - "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}]
        validateFirstItemInContext(firstItem)
          .flatMap(_ => {
            secondItem match {
              case contextBaseAndLangObject: ObjectNode =>
                getAndValidateBaseAndLangFromContextObject(
                  contextBaseAndLangObject,
                  baseUrl,
                  lang
                )
              case _ =>
                Left(
                  MetadataError(
                    "Second @context array value must be an object"
                  )
                )
            }
          })
      case Array(firstItem) =>
        // If @context contains just one element, the namespace for csvw should always be http://www.w3.org/ns/csvw
        // "@context": "http://www.w3.org/ns/csvw"
        validateFirstItemInContext(firstItem)
          .map(_ => (baseUrl, lang, Array[WarningWithCsvContext]()))
      case _ =>
        Left(
          MetadataError(s"Unexpected @context array length ${context.size()}")
        )
    }
  }

  /**
    * This function validates the second item in context property.
    * The second element can be @language or @base - "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}]
    * @param contextBaseAndLangObject - The context object.
    * @param baseUrl - The base URL of the CSV-W
    * @param lang - The language.
    * @return newBaseUrl, newLang, warnings (if any)
    */
  def getAndValidateBaseAndLangFromContextObject(
      contextBaseAndLangObject: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[(String, String, Array[WarningWithCsvContext])] = {
    val acc: Either[
      MetadataError,
      (String, String, Array[WarningWithCsvContext])
    ] = Right((baseUrl, lang, Array[WarningWithCsvContext]()))
    contextBaseAndLangObject.getKeysAndValues
      .foldLeft(acc)({
        case (err @ Left(_), _) => err
        case (Right((baseUrl, lang, warnings)), (property, value)) =>
          property match {
            case "@base" | "@language" =>
              PropertyChecker
                .parseJsonProperty(property, value, baseUrl, lang) match {
                case (propertyNode, Array(), _) =>
                  val propertyTextValue = propertyNode.asText()
                  property match {
                    case "@base" => Right((propertyTextValue, lang, warnings))
                    case "@language" =>
                      Right((baseUrl, propertyTextValue, warnings))
                    case _ =>
                      Left(
                        MetadataError(s"Unhandled context property '$property'")
                      )
                  }
                case (_, ws, _) =>
                  // There are warnings, don't update any properties.
                  Right(
                    (
                      baseUrl,
                      lang,
                      warnings ++ ws.map(
                        WarningWithCsvContext(
                          _,
                          "metadata",
                          "",
                          "",
                          s"$property: $value",
                          ""
                        )
                      )
                    )
                  )
              }
            case _ =>
              Left(
                MetadataError(
                  s"@context contains properties other than @base or @language $property)"
                )
              )
          }
      })
  }

  private def parseForeignKeysLinkToReferencedTables(
      baseUrl: String,
      tables: Map[String, Table]
  ): ParseResult[Map[String, Table]] = {
    tables.map({
      case (tableUrl, table) =>
        table.schema.map(tableSchema => {
          tableSchema.foreignKeys.zipWithIndex({
            case (foreignKey: ChildTableForeignKey, ind) => {
              foreignKey.jsonObject.getMaybeNode("reference")
                .toRight(Left(MetadataError(s"Foreign key reference node unset on '$tableUrl' foreign key at index $ind.")))
                .flatMap({
                  case referenceObjectNode: ObjectNode => Right(referenceObjectNode)
                  case referenceNode => Left(MetadataError(s"Foreign Key reference was not an object: ${referenceNode.toPrettyString}"))
                })
                .map(referenceNode => {
                  getReferencedTableForForeignKey(
                    baseUrl,
                    tables,
                    tableUrl,
                    ind,
                    referenceNode
                  ).map(parentTable => {
                    parentTable.schema.map(parentSchema => {
                      val mapNameToColumn = parentSchema.columns
                        .flatMap(col => col.name.map((_, col)))
                        .toMap

                      // todo: continue from here.
                      val parentReferencedColumns: Array[Column] = referenceNode
                        .get("columnReference")
                        .elements()
                        .asScalaArray
                        .map(columnReference => {
                          mapNameToColumn.get(columnReference.asText()) match {
                            case Some(column) => column
                            case None =>
                              throw MetadataError(
                                s"column named ${columnReference
                                  .asText()} does not exist in ${parentTable.url}," +
                                  s" $$.tables[?(@.url = '$tableUrl')].tableSchema.foreign_keys[$ind].reference.columnReference"
                              )
                          }
                        })

                      val foreignKeyWithTable =
                        ParentTableForeignKeyReference(
                          foreignKey,
                          parentTable,
                          parentReferencedColumns,
                          table
                        )
                      parentTable.foreignKeyReferences :+= foreignKeyWithTable
                      tables += (parentTable.url -> parentTable)
                    })
                })
            }
          })
        })
        .getOrElse(Right())
    })
  }

  private def getReferencedTableForForeignKey(
    baseUrl: String,
    tables: Map[String, Table],
    tableUrl: String,
    foreignKeyArrayIndex: Int,
    referenceObject: ObjectNode
  ): ParseResult[Table] = {
    referenceObject.getMaybeNode("resource")
      .map(resourceNode => {
        val referencedTableUrl = new URL(
          new URL(baseUrl),
          resourceNode.asText()
        ).toString
        tables.get(referencedTableUrl).map(Right(_)).getOrElse(Left(MetadataError(
          s"Could not find foreign key referenced table $referencedTableUrl, " +
            s"$$.tables[?(@.url = '$tableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.resource"
        )))
      })
      .getOrElse(
        referenceObject.getMaybeNode("schemaReference")
          .map(schemaReferenceNode => {
            val schemaUrl =
              new URL(new URL(baseUrl), schemaReferenceNode.asText()).toString
            tables.values
              .filter(table => table.schema.exists(s => s.schemaId.exists(_ == schemaUrl)))
              .toList.headOption
              .map(Right(_))
              .getOrElse(Left(MetadataError(
                s"Could not find foreign key referenced schema $schemaUrl, " +
                  s"$$.tables[?(@.url = '$tableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.SchemaReference"
              )))
          })
      )
  }

  private def parseTableGroupType(tableGroupNode: ObjectNode): ParseResult[String] = {
    tableGroupNode.getMaybeNode("@type").map(typeNode => {
      val allegedType = typeNode.asText
      if (allegedType == "TableGroup")
        Right(allegedType)
      else
        Left(MetadataError(s"@type of table group is not 'TableGroup', found @type to be a '$allegedType'"))
    })
      .getOrElse(Right("TableGroup"))
 }

  def partitionTableGroupProperties(
      tableGroupNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[PartitionedTableGroupProperties] = {
    tableGroupNode.getKeysAndValues.map({
      case (propertyName, valueNode) if validProperties.contains(propertyName) => Right((propertyName, valueNode, Array[String](), PropertyType.Common))
      case (propertyName, valueNode) =>
        PropertyChecker.parseJsonProperty(propertyName, valueNode, baseUrl, lang)
          .map(propertyName +: _)
    })
      .foldLeft[ParseResult[PartitionedTableGroupProperties]](Right(PartitionedTableGroupProperties()))({
        case (err@Left(_), _) => err
        case (_, Left(newErr)) => Left(newErr)
        case (Right(partitionedProperties), Right((propertyName, parsedValue, stringWarnings, propertyType))) =>
          val warnings = partitionedProperties.warnings ++ stringWarnings.map(WarningWithCsvContext(
            _,
            "metadata",
            "",
            "",
            s"$propertyName : ${parsedValue.toPrettyString}",
            ""
          ))
          propertyType match {
            case PropertyType.Annotation => Right(partitionedProperties.copy(annotations = partitionedProperties.annotations + (propertyName -> parsedValue), warnings = warnings))
            case PropertyType.Common => Right(partitionedProperties.copy(common = partitionedProperties.common + (propertyName -> parsedValue), warnings = warnings))
            case PropertyType.Inherited => Right(partitionedProperties.copy(inherited = partitionedProperties.inherited + (propertyName -> parsedValue), warnings = warnings))
            case _ => Right(partitionedProperties.copy(warnings = warnings :+ WarningWithCsvContext(
              "invalid_property",
              "metadata",
              "",
              "",
              propertyName,
              ""
            )))
          }
      })
  }

  private def parseTables(
      tableGroupNode: ObjectNode,
      baseUrl: String,
      lang: String,
      commonProperties: Map[String, JsonNode],
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[WithWarningsAndErrors[Map[String, Table]]] = {
    tableGroupNode.getMaybeNode("tables").map({
      case tablesArrayNode: ArrayNode if tablesArrayNode.isEmpty() => Left(MetadataError("Empty tables property"))
      case tablesArrayNode: ArrayNode =>
        parseArrayNodeTables(
          tablesArrayNode,
          baseUrl,
          lang,
          commonProperties,
          inheritedProperties
        )
      case _                    => Left(MetadataError("Tables property is not an array"))
    }).getOrElse(Left(MetadataError("No tables property")))
  }

  def parseArrayNodeTables(
      tablesArrayNode: ArrayNode,
      baseUrl: String,
      lang: String,
      commonProperties: Map[String, JsonNode],
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[WithWarningsAndErrors[Map[String, Table]]] = {
    val warnings = ArrayBuffer.empty[WarningWithCsvContext]
    val errors = ArrayBuffer.empty[ErrorWithCsvContext]
    val tables = mutable.Map[String, Table]()
    for (tableElement <- tablesArrayNode.elements().asScalaArray) {
      tableElement match {
        case tableElementObject: ObjectNode =>
          var tableUrl = tableElement.get("url")
          if (!tableUrl.isTextual) {
            errors.addOne(
              ErrorWithCsvContext(
                "invalid_url",
                "metadata",
                "",
                "",
                s"url: $tableUrl",
                ""
              )
            )
            tableUrl = new TextNode("")
          }
          tableUrl = new TextNode(
            new URL(new URL(baseUrl), tableUrl.asText()).toString
          )
          tableElementObject.set("url", tableUrl)
          val (table, w) = Table.fromJson(
            tableElementObject,
            baseUrl,
            lang,
            commonProperties,
            inheritedProperties
          )
          tables += (tableUrl.asText -> table)
          warnings.addAll(w)
        case _ =>
          warnings.addOne(
            WarningWithCsvContext(
              "invalid_table_description",
              "metadata",
              "",
              "",
              s"Value must be instance of object, found: ${tableElement.toPrettyString}",
              ""
            )
          )
      }
    }
    (
      tables,
      WarningsAndErrors(errors = errors.toArray, warnings = warnings.toArray)
    )
  }

  private def getId(commonProperties: Map[String, JsonNode]): Option[String] = {
    commonProperties
      .get("@id")
      .map(_.asText)
  }

  case class PartitionedTableGroupProperties(
                                              annotations: Map[String, JsonNode] = Map(),
                                              common: Map[String, JsonNode] = Map(),
                                              inherited: Map[String, JsonNode] = Map(),
                                              warnings: Array[WarningWithCsvContext] = Array()
                                       )

}
case class TableGroup private (
    baseUrl: String,
    id: Option[String],
    tables: Map[String, Table],
    notes: Option[JsonNode],
    annotations: Map[String, JsonNode]
) {

  def validateHeader(
      header: CSVRecord,
      tableUrl: String
  ): WarningsAndErrors = tables(tableUrl).validateHeader(header)
}
