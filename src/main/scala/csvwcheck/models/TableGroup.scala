package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{
  ArrayNode,
  JsonNodeFactory,
  ObjectNode,
  TextNode
}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{
  ErrorWithCsvContext,
  MetadataError,
  WarningWithCsvContext
}
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import csvwcheck.{PropertyChecker, models}
import org.apache.commons.csv.CSVRecord

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
  ): Either[MetadataError, ParsedResult[TableGroup]] = {
    var baseUrl = baseUri.trim
    var errors = Array[ErrorWithCsvContext]()
    var warnings = Array[WarningWithCsvContext]()
    if (containsWhitespaces.matches(baseUrl)) {
      println(
        "Warning: The path/url has whitespaces in it, please ensure its correctness. Proceeding with received " +
          "path/url .."
      )
    }
    val (baseUrl, lang, ws) =
      processContextGetBaseUrlLang(tableGroupNodeIn, baseUrl, "und")
    warnings ++= ws
    val tableGroupNode = restructureIfNodeIsSingleTable(tableGroupNodeIn)

    val (annotations, commonProperties, inheritedProperties, w1) =
      classifyPropertiesBasedOnPropertyTypeAndSetWarnings(
        tableGroupNode,
        baseUrl,
        lang
      )
    warnings = Array.concat(warnings, w1)
    val id = getId(commonProperties)
    ensureTypeofTableGroup(tableGroupNode)

    val (tables, warningsAndErrors) = createTableObjectsAndSetWarnings(
      tableGroupNode,
      baseUrl,
      lang,
      commonProperties,
      inheritedProperties
    )
    warnings = warnings.concat(warningsAndErrors.warnings)
    errors = errors.concat(warningsAndErrors.errors)

    findForeignKeysLinkToReferencedTables(baseUrl, tables)

    val tableGroup = TableGroup(
      baseUrl,
      id,
      tables,
      commonProperties.get("notes"),
      annotations
    )

    ParsedResult(
      tableGroup,
      models.WarningsAndErrors(warnings = warnings, errors = errors)
    )
  }

  private def restructureIfNodeIsSingleTable(
      tableGroupNode: ObjectNode
  ): ObjectNode = {
    if (tableGroupNode.path("tables").isMissingNode) {
      if (!tableGroupNode.path("url").isMissingNode) {
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
  ): Either[MetadataError, (String, String, Array[WarningWithCsvContext])] = {
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
  ): Either[MetadataError, (String, String, Array[WarningWithCsvContext])] = {
    def validateFirstItemInContext(
        firstItem: JsonNode
    ): Either[MetadataError, Unit] = {
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
  ): Either[MetadataError, (String, String, Array[WarningWithCsvContext])] = {
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
                .checkProperty(property, value, baseUrl, lang) match {
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

  private def findForeignKeysLinkToReferencedTables(
      baseUrl: String,
      tables: mutable.Map[String, Table]
  ): Unit = {
    for ((tableUrl, table) <- tables) {
      table.schema.map(s => {
        for ((foreignKey, i) <- s.foreignKeys.zipWithIndex) {
          val reference = foreignKey.jsonObject.get("reference")
          val parentTable: Table = setReferencedTableOrThrowException(
            baseUrl,
            tables,
            tableUrl,
            i,
            reference
          )
          val mapNameToColumn: mutable.Map[String, Column] = mutable.Map()
          parentTable.schema.map(parentSchema => {
            for (column <- parentSchema.columns) {
              column.name
                .foreach(columnName =>
                  mapNameToColumn += (columnName -> column)
                )
            }
          })

          val parentReferencedColumns: Array[Column] = reference
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
                      s" $$.tables[?(@.url = '$tableUrl')].tableSchema.foreign_keys[$i].reference.columnReference"
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
        }
      })
    }
  }

  private def setReferencedTableOrThrowException(
      baseUrl: String,
      tables: mutable.Map[String, Table],
      tableUrl: String,
      foreignKeyArrayIndex: Int,
      reference: JsonNode
  ): Table = {
    val resourceNode = reference.path("resource")
    if (resourceNode.isMissingNode) {
      val schemaReferenceNode =
        reference.get(
          "schemaReference"
        ) // Perform more checks and provide useful error messages if schemaReference is not present
      val schemaUrl =
        new URL(new URL(baseUrl), schemaReferenceNode.asText()).toString
      val referencedTables = List.from(
        tables.values
          .filter(t =>
            t.schema
              .flatMap(s => s.schemaId)
              .exists(schemaId => schemaId == schemaUrl)
          )
      )
      referencedTables match {
        case referencedTable :: _ => referencedTable
        case Nil =>
          throw MetadataError(
            s"Could not find foreign key referenced schema $schemaUrl, " +
              s"$$.tables[?(@.url = '$tableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.SchemaReference"
          )
      }
    } else {
      val referencedTableUrl = new URL(
        new URL(baseUrl),
        resourceNode.asText()
      ).toString
      tables.get(referencedTableUrl) match {
        case Some(refTable) =>
          refTable
        case None =>
          throw MetadataError(
            s"Could not find foreign key referenced table $referencedTableUrl, " +
              s"$$.tables[?(@.url = '$tableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.resource"
          )
      }
    }
  }

  private def ensureTypeofTableGroup(tableGroupNode: ObjectNode): Unit = {
    val typeNode = tableGroupNode.path("@type")
    if (!typeNode.isMissingNode && typeNode.asText != "TableGroup") {
      throw MetadataError(
        s"@type of table group is not 'TableGroup', found @type to be a '${typeNode.asText()}'"
      )
    }
  }

  def classifyPropertiesBasedOnPropertyTypeAndSetWarnings(
      tableGroupNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): (
      mutable.Map[String, JsonNode],
      mutable.Map[String, JsonNode],
      mutable.Map[String, JsonNode],
      Array[WarningWithCsvContext]
  ) = {
    val annotations = mutable.Map[String, JsonNode]()
    val commonProperties = mutable.Map[String, JsonNode]()
    val inheritedProperties = mutable.Map[String, JsonNode]()
    val warnings = ArrayBuffer.empty[WarningWithCsvContext]
    for ((property, value) <- tableGroupNode.getKeysAndValues) {
      if (!validProperties.contains(property)) {
        val (newValue, w, csvwPropertyType) =
          PropertyChecker.checkProperty(property, value, baseUrl, lang)
        warnings.addAll(
          w.map(x =>
            WarningWithCsvContext(
              x,
              "metadata",
              "",
              "",
              s"$property : ${value.toPrettyString}",
              ""
            )
          )
        )
        csvwPropertyType match {
          case PropertyType.Annotation => annotations += (property -> newValue)
          case PropertyType.Common     => commonProperties += (property -> newValue)
          case PropertyType.Inherited =>
            inheritedProperties += (property -> newValue)
          case _ =>
            warnings.addOne(
              WarningWithCsvContext(
                "invalid_property",
                "metadata",
                "",
                "",
                property,
                ""
              )
            )
        }
      }
      ()
    }
    (annotations, commonProperties, inheritedProperties, warnings.toArray)
  }

  private def createTableObjectsAndSetWarnings(
      tableGroupNode: ObjectNode,
      baseUrl: String,
      lang: String,
      commonProperties: mutable.Map[String, JsonNode],
      inheritedProperties: mutable.Map[String, JsonNode]
  ): (mutable.Map[String, Table], WarningsAndErrors) = {
    tableGroupNode.path("tables") match {
      case t: ArrayNode if t.isEmpty() =>
        throw MetadataError("Empty tables property")
      case t: ArrayNode =>
        extractTablesFromNode(
          t,
          baseUrl,
          lang,
          commonProperties,
          inheritedProperties
        )
      case n if n.isMissingNode => throw MetadataError("No tables property")
      case _                    => throw MetadataError("Tables property is not an array")
    }
  }

  def extractTablesFromNode(
      tablesArrayNode: ArrayNode,
      baseUrl: String,
      lang: String,
      commonProperties: mutable.Map[String, JsonNode],
      inheritedProperties: mutable.Map[String, JsonNode]
  ): (mutable.Map[String, Table], WarningsAndErrors) = {
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

  private def getId(commonProperties: mutable.Map[String, JsonNode]) = {
    commonProperties
      .get("@id")
      .map(idNode => idNode.asText())
  }

}
case class TableGroup private (
    baseUrl: String,
    id: Option[String],
    tables: mutable.Map[String, Table],
    notes: Option[JsonNode],
    annotations: mutable.Map[String, JsonNode]
) {

  def validateHeader(
      header: CSVRecord,
      tableUrl: String
  ): WarningsAndErrors = tables(tableUrl).validateHeader(header)
}
