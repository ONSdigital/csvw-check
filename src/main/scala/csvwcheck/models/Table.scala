package csvwcheck.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.typesafe.scalalogging.Logger
import csvwcheck.PropertyChecker.StringWarnings
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import csvwcheck.{PropertyChecker, models}
import csvwcheck.traits.LoggerExtensions.LogDebugException

import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import shapeless.syntax.std.tuple.productTupleOps

import java.io.File
import java.net.URI
import java.nio.charset.Charset
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.control.NonFatal
object Table {
  type MapForeignKeyDefinitionToValues = Map[ForeignKeyDefinition, Set[KeyWithContext]]
  type MapForeignKeyReferenceToValues = Map[ReferencedTableForeignKeyReference, Set[KeyWithContext]]
  type PrimaryKeysAndErrors = (mutable.Set[List[Any]], ArrayBuffer[ErrorWithCsvContext])

  private val tablePermittedProperties = Array[String](
    "columns",
    "primaryKey",
    "foreignKeys",
    "rowTitles"
  )

  def fromJson(
      tableDesc: ObjectNode,
      baseUrl: String,
      lang: String,
      commonProperties: Map[String, JsonNode],
      inheritedPropertiesIn: Map[String, JsonNode]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    partitionAndValidateTablePropertiesByType(
      commonProperties,
      inheritedPropertiesIn,
      tableDesc,
      baseUrl,
      lang
    ).flatMap({
      case PartitionedTableProperties(
            annotations,
            tableProperties,
            inheritedProperties,
            warnings
          ) =>
        getUrl(tableProperties).flatMap(url =>
          extractTableSchema(tableProperties, inheritedProperties) match {
            case Some(tableSchema) =>
              parseTable(
                tableSchema,
                inheritedProperties,
                baseUrl,
                lang,
                url,
                tableProperties,
                annotations,
                warnings
              )
            case _ =>
              Right(
                initializeTableWithDefaults(
                  annotations,
                  warnings,
                  url
                )
              )
          }
        )
    })
  }

  private def partitionAndValidateTablePropertiesByType(
      commonProperties: Map[String, JsonNode],
      inheritedProperties: Map[String, JsonNode],
      tableObjectNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[PartitionedTableProperties] = {
    val initialPartitionedProperties = PartitionedTableProperties(
      tableProperties = commonProperties,
      inheritedProperties = inheritedProperties
    )

    tableObjectNode.getKeysAndValues
      .map(parseTableProperty(_, tableObjectNode, baseUrl, lang))
      .foldLeft[ParseResult[PartitionedTableProperties]](
        Right(initialPartitionedProperties)
      )({
        case (err @ Left(_), _) => err
        case (_, Left(newErr))  => Left(newErr)
        case (
              Right(partitionedProperties),
              Right((propertyName, parsedNode, stringWarnings, propertyType))
            ) =>
          val warnings: Array[WarningWithCsvContext] =
            partitionedProperties.warnings ++ stringWarnings.map(
              WarningWithCsvContext(
                _,
                "metadata",
                "",
                "",
                s"$propertyName : $parsedNode",
                ""
              )
            )
          propertyType match {
            case PropertyType.Annotation =>
              Right(
                partitionedProperties.copy(
                  annotations =
                    partitionedProperties.annotations + (propertyName -> parsedNode),
                  warnings = warnings
                )
              )
            case PropertyType.Table | PropertyType.Common =>
              Right(
                partitionedProperties.copy(
                  tableProperties =
                    partitionedProperties.tableProperties + (propertyName -> parsedNode),
                  warnings = warnings
                )
              )
            case PropertyType.Column =>
              Right(
                partitionedProperties.copy(warnings =
                  warnings :+ WarningWithCsvContext(
                    "invalid_property",
                    "metadata",
                    "",
                    "",
                    propertyName,
                    ""
                  )
                )
              )
            case _ =>
              Right(
                partitionedProperties.copy(
                  inheritedProperties =
                    partitionedProperties.inheritedProperties + (propertyName -> parsedNode),
                  warnings = warnings
                )
              )
          }

      })
  }

  private def parseTableProperty(
      propertyNameAndValue: (String, JsonNode),
      tableObjectNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[(String, JsonNode, StringWarnings, PropertyType.Value)] =
    propertyNameAndValue match {
      case ("@type", textNode: TextNode) if textNode.asText == "Table" =>
        Right("@type", textNode, Array[String](), PropertyType.Common)
      case ("@type", _: TextNode) =>
        val tableUrl = tableObjectNode
          .getMaybeNode("url")
          .map(_.asText)
          .getOrElse("UnknownURL")
        Left(
          MetadataError(
            s"@type of table is not 'Table' - $tableUrl.@type"
          )
        )
      case ("@type", value) =>
        Left(
          MetadataError(
            s"Unexpected value for '@type'. Expected string but got ${value.getNodeType} (${value.toPrettyString})"
          )
        )
      case (propertyName, value) =>
        PropertyChecker
          .parseJsonProperty(propertyName, value, baseUrl, lang)
          .map(propertyName +: _)
    }

  private def getUrl(
      tableProperties: Map[String, JsonNode]
  ): ParseResult[String] = {
    tableProperties
      .get("url")
      .map(urlNode => Right(urlNode.asText()))
      .getOrElse(Left(MetadataError("URL not found for table")))
  }

  private def extractTableSchema(
      tableProperties: Map[String, JsonNode],
      inheritedPropertiesCopy: Map[String, JsonNode]
  ): Option[JsonNode] = {
    tableProperties
      .get("tableSchema")
      .orElse(inheritedPropertiesCopy.get("tableSchema"))
  }

  private def parseTable(
      tableSchema: JsonNode,
      inheritedProperties: Map[String, JsonNode],
      baseUrl: String,
      lang: String,
      tableUrl: String,
      tableProperties: Map[String, JsonNode],
      annotations: Map[String, JsonNode],
      existingWarnings: Array[WarningWithCsvContext]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    tableSchema match {
      case tableSchemaObject: ObjectNode =>
        val inheritedPropertiesWithPermittedTableProperties =
          getTableSchemaInheritedProperties(
            inheritedProperties,
            tableSchemaObject
          )

        val partiallyParsedTable = new Table(
          url = tableUrl,
          id = tableProperties.get("@id").map(_.asText()),
          suppressOutput = tableProperties
            .get("suppressOutput")
            .map(_.asBoolean)
            .getOrElse(false),
          annotations = annotations
        )

        parseTableSchema(
          partiallyParsedTable,
          tableSchemaObject,
          inheritedPropertiesWithPermittedTableProperties,
          baseUrl,
          lang
        ).flatMap(parseDialect(_, tableProperties))
          .flatMap(parseNotes(_, tableProperties))
          .map({
            case (table, warnings) => (table, warnings ++ existingWarnings)
          })
      case _ =>
        Left(
          MetadataError(
            s"Table schema must be object for table $tableUrl "
          )
        )
    }
  }

  private def parseTableSchema(
      table: Table,
      tableSchemaObject: ObjectNode,
      tablePropertiesIncludingInherited: Map[String, JsonNode],
      baseUrl: String,
      lang: String
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    parseAndValidateColumns(
      table.url,
      baseUrl,
      lang,
      tablePropertiesIncludingInherited,
      tableSchemaObject
    ).flatMap({
      case (columns, columnWarnings) =>
        parseTableSchemaGivenColumns(
          table,
          tableSchemaObject,
          columns,
          columnWarnings
        )
    })
  }

  private def parseTableSchemaGivenColumns(
      table: Table,
      tableSchemaObject: ObjectNode,
      columns: Array[Column],
      warnings: Array[WarningWithCsvContext]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    parseRowTitles(tableSchemaObject, columns)
      .flatMap(rowTitleColumns =>
        parseForeignKeyColumns(tableSchemaObject, columns)
          .map(foreignKeyMappings => (rowTitleColumns, foreignKeyMappings))
      )
      .flatMap({
        case (rowTitleColumns, foreignKeyMappings) =>
          parsePrimaryKeyColumns(tableSchemaObject, columns)
            .map((rowTitleColumns, foreignKeyMappings) ++ _)
      })
      .map({
        case (rowTitleColumns, foreignKeyMappings, pkColumns, pkWarnings) =>
          // tableSchemas defined in other files have been updated to inline representations by this point already.
          // See propertyChecker function for tableSchema.
          val tableSchema = TableSchema(
            columns = columns,
            foreignKeys = foreignKeyMappings,
            primaryKey = pkColumns,
            rowTitleColumns = rowTitleColumns,
            schemaId = tableSchemaObject.getMaybeNode("@id").map(_.asText)
          )

          (
            table.copy(schema = Some(tableSchema)),
            warnings ++ pkWarnings
          )
      })
  }

  private def parseRowTitles(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): ParseResult[Array[Column]] = {
    tableSchemaObject
      .getMaybeNode("rowTitles")
      .map({
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScala
            .map({
              case rowTitleElement: TextNode =>
                val rowTitle = rowTitleElement.asText
                columns
                  .find(col => col.name.exists(_ == rowTitle))
                  .map(Right(_))
                  .getOrElse(
                    Left(
                      MetadataError(
                        s"rowTitles references non-existent column - '$rowTitle''"
                      )
                    )
                  )
              case rowTitleElement =>
                Left(
                  MetadataError(
                    s"Unhandled rowTitle value '${rowTitleElement.toPrettyString}'"
                  )
                )
            })
            .foldLeft[ParseResult[Array[Column]]](Right(Array()))({
              case (err @ Left(_), _) => err
              case (_, Left(newErr))  => Left(newErr)
              case (Right(rowTitleColumns), Right(newRowTitleColumn)) =>
                Right(rowTitleColumns :+ newRowTitleColumn)
            })
        case rowTitlesNode =>
          Left(
            MetadataError(
              s"Unsupported rowTitles value ${rowTitlesNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(Array[Column]()))
  }

  private def parseForeignKeyColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): ParseResult[Array[ForeignKeyDefinition]] = {
    tableSchemaObject
      .getMaybeNode("foreignKeys")
      .map({
        case foreignKeysNode: ArrayNode =>
          foreignKeysNode
            .elements()
            .asScala
            .map({
              case foreignKeyObjectNode: ObjectNode =>
                parseForeignKeyObjectNode(foreignKeyObjectNode, columns)
              case foreignKeyNode =>
                Left(
                  MetadataError(
                    s"Unexpected foreign key value ${foreignKeyNode.toPrettyString}"
                  )
                )
            })
            .foldLeft[ParseResult[Array[ForeignKeyDefinition]]](Right(Array()))({
              case (err @ Left(_), _) => err
              case (_, Left(newErr))  => Left(newErr)
              case (Right(foreignKeys), Right(newForeignKey)) =>
                Right(foreignKeys :+ newForeignKey)
            })
        case foreignKeysNode =>
          Left(
            MetadataError(
              s"Unexpected foreign keys node ${foreignKeysNode.toPrettyString}"
            )
          )
      })
      .getOrElse(Right(Array()))
  }

  private def parseForeignKeyObjectNode(
      foreignKeyObjectNode: ObjectNode,
      columns: Array[Column]
  ): ParseResult[ForeignKeyDefinition] = {
    foreignKeyObjectNode
      .getMaybeNode("columnReference")
      .map({
        case columnReferenceArrayNode: ArrayNode =>
          parseForeignKeyReferencesForColumnReferenceArrayNode(
            foreignKeyObjectNode,
            columnReferenceArrayNode,
            columns
          )
        case columnReferenceNode =>
          Left(
            MetadataError(
              s"columnReference property set to unexpected value ${columnReferenceNode.toPrettyString}"
            )
          )
      })
      .getOrElse(
        Left(
          MetadataError(
            s"columnReference property unset on foreignKey ${foreignKeyObjectNode.toPrettyString}"
          )
        )
      )
  }

  private def parseForeignKeyReferencesForColumnReferenceArrayNode(
      foreignKeyObjectNode: ObjectNode,
      columnReferenceArrayNode: ArrayNode,
      columns: Array[Column]
  ): ParseResult[ForeignKeyDefinition] = {
    columnReferenceArrayNode
      .elements()
      .asScala
      .map({
        case columnReferenceElementTextNode: TextNode =>
          val columnReference = columnReferenceElementTextNode.asText
          columns
            .find(col => col.name.exists(_ == columnReference))
            .map(Right(_))
            .getOrElse(
              Left(
                MetadataError(
                  s"foreignKey references non-existent column - $columnReference"
                )
              )
            )
        case columnReferenceElement =>
          Left(
            MetadataError(
              s"columnReference element set to unexpected value ${columnReferenceElement.toPrettyString}"
            )
          )
      })
      .foldLeft[ParseResult[Array[Column]]](Right(Array()))({
        case (err @ Left(_), _) => err
        case (_, Left(newErr))  => Left(newErr)
        case (Right(foreignKeyColumns), Right(newForeignKeyColumn)) =>
          Right(foreignKeyColumns :+ newForeignKeyColumn)
      })
      .map(foreignKeyColumns =>
        ForeignKeyDefinition(foreignKeyObjectNode, foreignKeyColumns)
      )
  }

  private def parsePrimaryKeyColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] = {
    tableSchemaObject
      .getMaybeNode("primaryKey")
      .map(primaryKeyNode =>
        primaryKeyNode
          .elements()
          .asScala
          .map(parsePrimaryKeyColumnReference(_, columns))
          .foldLeft[(Option[Array[Column]], Array[WarningWithCsvContext])](
            Some(Array[Column]()),
            Array[WarningWithCsvContext]()
          )(
            {
              case ((_, warnings), Left(newErr)) =>
                (
                  None,
                  warnings :+ newErr
                ) // Primary key is now invalid, return no columns.
              case ((None, warnings), Right(_)) =>
                (
                  None,
                  warnings
                ) // Primary key is already invalid, return no columns.
              case (
                    (Some(primaryKeyColumns), warnings),
                    Right(newPrimaryKeyColumn)
                  ) =>
                (Some(primaryKeyColumns :+ newPrimaryKeyColumn), warnings)
            }
          )
      )
      .map({
        case (None, warnings) =>
          Right(
            (Array[Column](), warnings)
          ) // Primary key definition was invalid.
        case (Some(primaryKeyColumns), warnings) =>
          Right((primaryKeyColumns, warnings))
      })
      .getOrElse(Right((Array[Column](), Array.empty)))
  }

  private def parsePrimaryKeyColumnReference(
      primaryKeyArrayElement: JsonNode,
      columns: Array[Column]
  ): Either[WarningWithCsvContext, Column] = {
    primaryKeyArrayElement match {
      case primaryKeyElement: TextNode =>
        val primaryKeyColNameReference = primaryKeyElement.asText()
        columns
          .find(col => col.name.exists(_ == primaryKeyColNameReference))
          .map(Right(_))
          .getOrElse(
            Left(
              WarningWithCsvContext(
                "invalid_column_reference",
                "metadata",
                "",
                "",
                s"primaryKey: $primaryKeyColNameReference",
                ""
              )
            )
          )
      case primaryKeyElement =>
        Left(
          WarningWithCsvContext(
            "invalid_column_reference",
            "metadata",
            "",
            "",
            s"primaryKey: ${primaryKeyElement.toPrettyString}",
            ""
          )
        )
    }
  }

  private def parseAndValidateColumns(
      tableUrl: String,
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode],
      tableSchemaObject: ObjectNode
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] =
    parseColumnDefinitions(
      tableUrl,
      tableSchemaObject,
      baseUrl,
      lang,
      inheritedProperties
    ).flatMap(ensureNoDuplicateColumnNames)
      .flatMap(ensureVirtualColumnsAfterColumns)

  private def parseColumnDefinitions(
      tableUrl: String,
      tableSchemaObject: ObjectNode,
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] = {
    tableSchemaObject
      .getMaybeNode("columns")
      .map({
        case arrayNode: ArrayNode =>
          arrayNode
            .elements()
            .asScalaArray
            .zipWithIndex
            .map(parseColumnDefinition(_, baseUrl, lang, inheritedProperties))
            .foldLeft[ParseResult[
              (Array[Column], Array[WarningWithCsvContext])
            ]](Right(Array(), Array()))({
              case (err @ Left(_), _)  => err
              case (_, Left(newError)) => Left(newError)
              case (
                    Right((columns, warnings)),
                    Right((Some(column), newWarnings))
                  ) =>
                Right(columns :+ column, warnings ++ newWarnings)
              case (Right((columns, warnings)), Right((None, newWarnings))) =>
                Right(columns, warnings ++ newWarnings)
            })
        case _ =>
          Right(
            (
              Array[Column](),
              Array(
                WarningWithCsvContext(
                  "invalid_value",
                  "metadata",
                  "",
                  "",
                  s"columns is not array for table: $tableUrl",
                  ""
                )
              )
            )
          )
      })
      .getOrElse(Right((Array.empty, Array.empty)))
  }

  def parseColumnDefinition(
      colWithIndex: (JsonNode, Int),
      baseUrl: String,
      lang: String,
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[(Option[Column], Array[WarningWithCsvContext])] =
    colWithIndex match {
      case (col, index) =>
        col match {
          case colObj: ObjectNode =>
            val colNum = index + 1
            Column
              .fromJson(
                colNum,
                colObj,
                baseUrl,
                lang,
                inheritedProperties
              )
              .map({
                case (colDef, warningsWithoutContext) =>
                  (
                    Some(colDef),
                    warningsWithoutContext.map(warningWithoutContext =>
                      WarningWithCsvContext(
                        warningWithoutContext.`type`,
                        "metadata",
                        "",
                        colNum.toString,
                        warningWithoutContext.content,
                        ""
                      )
                    )
                  )
              })
          case _ =>
            Right(
              (
                None,
                Array(
                  WarningWithCsvContext(
                    "invalid_column_description",
                    "metadata",
                    "",
                    "",
                    col.toString,
                    ""
                  )
                )
              )
            )
        }
    }

  private def ensureNoDuplicateColumnNames(
      columnsAndWarnings: (Array[Column], Array[WarningWithCsvContext])
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] = {
    val (columns, _) = columnsAndWarnings
    val columnNames = columns.flatMap(c => c.name)

    val duplicateColumnNames = columnNames
      .groupBy(identity)
      .filter { case (_, elements) => elements.length > 1 }
      .keys

    if (duplicateColumnNames.nonEmpty) {
      Left(
        MetadataError(
          s"Multiple columns named ${duplicateColumnNames.mkString(", ")}"
        )
      )
    } else {
      Right(columnsAndWarnings)
    }
  }

  private def ensureVirtualColumnsAfterColumns(
      columnsAndWarnings: (Array[Column], Array[WarningWithCsvContext])
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] = {
    val (columns, _) = columnsAndWarnings
    var virtualColumns = false
    for (column <- columns) {
      if (virtualColumns && !column.virtual) {
        return Left(
          MetadataError(
            s"virtual columns before non-virtual column ${column.name.get} (${column.columnOrdinal})"
          )
        )
      }
      virtualColumns = virtualColumns || column.virtual
    }
    Right(columnsAndWarnings)
  }

  private def parseDialect(
      tableAndWarnings: (Table, Array[WarningWithCsvContext]),
      tableProperties: Map[String, JsonNode]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    val (table, warnings) = tableAndWarnings
    tableProperties
      .get("dialect")
      .map({
        case d: ObjectNode =>
          Dialect
            .fromJson(d)
            .map(dialect => (table.copy(dialect = Some(dialect)), warnings))
        case d if d.isMissingNode || d.isNull => Right(tableAndWarnings)
        case d =>
          Left(
            MetadataError(
              s"Unexpected JsonNode type ${d.getClass.getName}"
            )
          )
      })
      .getOrElse(Right(tableAndWarnings))

  }

  private def getTableSchemaInheritedProperties(
      inheritedProperties: Map[String, JsonNode],
      tableSchemaObject: ObjectNode
  ): Map[String, JsonNode] = {
    inheritedProperties ++ tableSchemaObject.getKeysAndValues
      .filter({
        case (propertyName, _) =>
          tablePermittedProperties.contains(propertyName)
      })
      .toMap
  }

  private def parseNotes(
      tableAndWarnings: (Table, Array[WarningWithCsvContext]),
      tableProperties: Map[String, JsonNode]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    val (table, warnings) = tableAndWarnings

    tableProperties
      .get("notes")
      .map({
        case notesNode: ArrayNode =>
          Right(table.copy(notes = Some(notesNode)), warnings)
        case _ => Left(MetadataError("Notes property should be an array"))
      })
      .getOrElse(Right(tableAndWarnings))
  }

  private def initializeTableWithDefaults(
      annotations: Map[String, JsonNode],
      warnings: Array[WarningWithCsvContext],
      url: String
  ): (Table, Array[WarningWithCsvContext]) = {
    val table = new Table(
      url = url,
      id = None,
      schema = None,
      dialect = None,
      notes = None,
      suppressOutput = false,
      annotations = annotations
    )
    (table, warnings)
  }

  case class PartitionedTableProperties private (
      annotations: Map[String, JsonNode] = Map(),
      tableProperties: Map[String, JsonNode] = Map(),
      inheritedProperties: Map[String, JsonNode] = Map(),
      warnings: Array[WarningWithCsvContext] = Array()
  )
}

case class TableSchema private (
                        columns: Array[Column],
                        primaryKey: Array[Column],
                        rowTitleColumns: Array[Column],
                        foreignKeys: Array[ForeignKeyDefinition],
                        schemaId: Option[String]
)

case class Table private (
    url: String,
    suppressOutput: Boolean,
    annotations: Map[String, JsonNode],
    id: Option[String] = None,
    schema: Option[TableSchema] = None,
    dialect: Option[Dialect] = None,
    notes: Option[ArrayNode] = None,
    // This array contains the foreign keys defined in other tables' schemas which reference data inside this table.
    foreignKeyReferences: Array[ReferencedTableForeignKeyReference] = Array()
) {
  import csvwcheck.models.Table.{MapForeignKeyDefinitionToValues, MapForeignKeyReferenceToValues, PrimaryKeysAndErrors}

  private val logger = Logger(this.getClass.getName)

  override def toString: String = s"Table($url)"

  override def hashCode(): Int = url.hashCode

  override def equals(obj: Any): Boolean =
    if (obj.isInstanceOf[Table]) {
      obj.asInstanceOf[Table].url == this.url
    } else {
      false
    }

  implicit val ec: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.global

  val mapAvailableCharsets: mutable.Map[String, Charset] =
    Charset.availableCharsets().asScala

  def parseCsv(degreeOfParallelism: Int, rowGrouping: Int): Source[
    (
      WarningsAndErrors,
      MapForeignKeyDefinitionToValues,
      MapForeignKeyReferenceToValues
    ),
    NotUsed
  ] = {
    val dialect = this.dialect.getOrElse(Dialect())
    val format = getCsvFormat(dialect)

    getParser(dialect, format) match {
      case Right(parser) =>
        readAndValidateTableWithParser(
          format,
          parser,
          dialect,
          degreeOfParallelism,
          rowGrouping
        ).recover {
          case NonFatal(err) =>
            logger.debug(err)
            val warnings = Array(
              WarningWithCsvContext(
                "source_url_mismatch",
                "CSV requested not found in metadata",
                "",
                "",
                s"Table URL: '$url'",
                ""
              )
            )

            (
              WarningsAndErrors(warnings = warnings),
              Map[ForeignKeyDefinition, Set[KeyWithContext]](),
              Map[ReferencedTableForeignKeyReference, Set[KeyWithContext]]()
            )
        }
      case Left(warningsAndErrors) =>
        Source(List(
          (
            warningsAndErrors,
            Map[ForeignKeyDefinition, Set[KeyWithContext]](),
            Map[ReferencedTableForeignKeyReference, Set[KeyWithContext]]()
          )
        ))
    }
  }

  def getCsvFormat(dialect: Dialect): CSVFormat = {
    var formatBuilder = CSVFormat.RFC4180
      .builder()
      .setDelimiter(dialect.delimiter)
      .setQuote(dialect.quoteChar)
      .setTrim(true) // Implement trim as per w3c spec, issue for this exists
      .setIgnoreEmptyLines(dialect.skipBlankRows)

    formatBuilder = if (dialect.doubleQuote) {
      // https://github.com/apache/commons-csv/commit/c025d73d31ca9c9c467f3bad142ca62d7ebee76b
      // Above link explains that escaping with a double-quote mark only works if you avoid specifying the escape character.
      // The default behaviour of CsvParser will ensure the escape functions correctly.
      formatBuilder
    } else {
      formatBuilder.setEscape('\\')
    }

    formatBuilder.build()
  }


  private def readAndValidateTableWithParser(
      format: CSVFormat,
      parser: CSVParser,
      dialect: Dialect,
      degreeOfParallelism: Int,
      rowGrouping: Int
  ): Source[
    (
      WarningsAndErrors,
        MapForeignKeyDefinitionToValues,
        MapForeignKeyReferenceToValues
      ),
    NotUsed
  ] = {
    Source
      .fromIterator(() => parser.asScala.iterator)
      .filter(row => row.getRecordNumber > dialect.skipRows)
      .grouped(rowGrouping)
      // If the actual computation time required for processing something is really low, the gains brought in by
      // parallelism could be overshadowed by the additional costs of the Akka Streams.
      // Validating the contents of a row is not an expensive task. So we parallelize this using Akka Streams,
      // yes it can do more rows at a time but the additional cost of managing all these processes almost takes away
      // the gains by parallelization.
      // To overcome this, we are grouping rows together so that each task to process is large enough
      // and thus better results are obtained. Grouped function allows accumulating the incoming elements
      // until a specified number has been reached
      .mapAsyncUnordered(degreeOfParallelism)(batchedCsvRows =>
        Future {
          batchedCsvRows
            .map(parseRow(_, dialect))
        }
      )
      .fold(AccumulatedTableKeyValues()) {
        case (accumulatedTableKeyValues, rowOutputs) =>
          accumulateTableKeyValuesForRowGroup(
            accumulatedTableKeyValues,
            rowOutputs
          )
      }
      .flatMapConcat(accumulatedTableKeyValues => {
        val parser: CSVParser = getParser(dialect, format)
          .getOrElse(
            throw new Exception("Could not fetch CSV parser. This should never happen.")
          )

        checkPossiblePrimaryKeyDuplicates(
          accumulatedTableKeyValues,
          parser,
          rowGrouping,
          degreeOfParallelism
        ).map(
          _ +: (accumulatedTableKeyValues.mapForeignKeyDefinitionToKeys, accumulatedTableKeyValues.mapForeignKeyReferenceToKeys)
        )
      })
  }

  private def getParser(dialect: Dialect, format: CSVFormat): Either[WarningsAndErrors, CSVParser] = {
    val tableUri = new URI(url)
    if (tableUri.getScheme == "file") {
      val tableCsvFile = new File(tableUri)
      if (!tableCsvFile.exists) {
        Left(
          WarningsAndErrors(
            Array(),
            Array(
              ErrorWithCsvContext(
                "file_not_found",
                "",
                "",
                "",
                s"File named ${tableUri.toString} cannot be located",
                ""
              )
            )
          )
        )
      }
      Right(
        CSVParser.parse(
          tableCsvFile,
          mapAvailableCharsets(dialect.encoding),
          format
        )
      )
    } else {
      try {
        val csvParser = CSVParser.parse(
          tableUri.toURL,
          mapAvailableCharsets(dialect.encoding),
          format
        )
        Right(csvParser)
      } catch {
        case NonFatal(e) =>
          logger.debug(e)
          Left(
            WarningsAndErrors(
              Array(
                WarningWithCsvContext(
                  "url_cannot_be_fetched",
                  "",
                  "",
                  "",
                  s"Url ${tableUri.toString} cannot be fetched",
                  ""
                )
              ),
              Array()
            )
          )
      }
    }
  }


  private def accumulateTableKeyValuesForRowGroup(
                                                   accumulatedTableKeyValues: AccumulatedTableKeyValues,
                                                   rowGroup: Seq[ValidateRowOutput]
                                                 ): AccumulatedTableKeyValues =
    rowGroup.foldLeft(accumulatedTableKeyValues)({
      case (acc, rowOutput) =>
        acc.copy(
          errors = acc.errors ++ rowOutput.warningsAndErrors.errors,
          warnings = acc.warnings ++ rowOutput.warningsAndErrors.warnings,
          mapForeignKeyDefinitionToKeys = accumulateOriginTableForeignKeyValues(
            rowOutput,
            acc.mapForeignKeyDefinitionToKeys
          ),
          mapForeignKeyReferenceToKeys =
            accumulateReferencedTableForeignKeyValues(
              rowOutput,
              acc.mapForeignKeyReferenceToKeys
            ),
          mapPrimaryKeyHashToRowNums = accumulatePossiblePrimaryKeyDuplicates(
            acc.mapPrimaryKeyHashToRowNums,
            rowOutput
          )
        )
    })

  /**
    * Since the primary key validation was done based on the hashes of primaryKeys in each row, there could be
    * primaryKey errors reported because of collisions.
    * For example primary key values of 2 different rows can have the same hash even when they are NOT the same.
    * This means that we could have false negatives in primaryKey errors.
    * To fix this, all the hashes which contain more than one rowNumber is checked again and the primary key error is set
    * at this point. During this checking the actual values of primary keys of these rows are compared.
    */
  private def checkPossiblePrimaryKeyDuplicates(
                                                 accumulatedTableKeyValues: AccumulatedTableKeyValues,
                                                 parser: CSVParser,
                                                 rowGrouping: Int,
                                                 parallelism: Int
                                               ): Source[
    WarningsAndErrors,
    NotUsed
  ] = {
    val rowsToCheckAgain = accumulatedTableKeyValues.mapPrimaryKeyHashToRowNums
      .filter { case (_, rowNums) => rowNums.length > 1 }
      .values
      .flatMap(_.toList)
      .toSet

    Source
      .fromIterator(() => parser.asScala.iterator)
      .filter(row => rowsToCheckAgain.contains(row.getRecordNumber))
      .grouped(rowGrouping)
      // If the actual computation time required for processing something is really low, the gains brought in by
      // parallelism could be overshadowed by the additional costs of the Akka Streams.
      // Validating the contents of a row is not an expensive task. So we parallelize this using Akka Streams,
      // yes it can do more rows at a time but the additional cost of managing all these processes almost takes away
      // the gains by parallelization.
      // To overcome this, we are grouping rows together so that each task to process is large enough
      // and thus better results are obtained. Grouped function allows accumulating the incoming elements
      // until a specified number has been reached
      .mapAsyncUnordered(parallelism)(csvRows =>
        Future {
          // Since every row is once validated, we do not need the whole checking inside parseRow Function again here.
          // We just need validateRowOutput object so that we can have the actual data for primary keys in each row.
          // This is the reason why we are using table.validateRow here instead of parseRow
          csvRows.map(validateRow)
        }
      )
      .fold[PrimaryKeysAndErrors](
        (mutable.Set[List[Any]](), ArrayBuffer.empty[ErrorWithCsvContext])
      ) {
        case (
          (
            primaryKeyValues,
            errorsInAkkaStreams
            ),
          rowOutputs: Seq[ValidateRowOutput]
          ) =>
          for (rowOutput <- rowOutputs) {
            ensurePrimaryKeyValueIsNotDuplicate(primaryKeyValues, rowOutput)
              .foreach(errorsInAkkaStreams.addOne(_))
          }
          (primaryKeyValues, errorsInAkkaStreams)
      }
      .map {
        case (_, err) =>
          WarningsAndErrors(
            accumulatedTableKeyValues.warnings,
            accumulatedTableKeyValues.errors ++ err
          )
      }
  }

  private def ensurePrimaryKeyValueIsNotDuplicate(
                                                   existingPrimaryKeyValues: mutable.Set[List[Any]],
                                                   validateRowOutput: ValidateRowOutput
                                                 ): Option[ErrorWithCsvContext] = {
    val primaryKeyValues = validateRowOutput.primaryKeyValues
    if (
      validateRowOutput.primaryKeyValues.nonEmpty && existingPrimaryKeyValues
        .contains(
          primaryKeyValues
        )
    ) {
      Some(
        ErrorWithCsvContext(
          "duplicate_key",
          "schema",
          validateRowOutput.recordNumber.toString,
          "",
          s"key already present - ${primaryKeyValues.mkString(", ")}",
          ""
        )
      )
    } else {
      existingPrimaryKeyValues += primaryKeyValues
      None
    }
  }

  private def parseRow(row: CSVRecord, dialect: Dialect): ValidateRowOutput = {
    if (row.getRecordNumber == 1 && dialect.header) {
      val warningsAndErrors = validateHeader(row)
      ValidateRowOutput(
        warningsAndErrors = warningsAndErrors,
        recordNumber = row.getRecordNumber
      )
    } else {
      if (row.size == 0) {
        val blankRowError = ErrorWithCsvContext(
          "Blank rows",
          "structure",
          row.getRecordNumber.toString,
          "",
          "",
          ""
        )
        val warningsAndErrors =
          WarningsAndErrors(errors = Array(blankRowError))
        ValidateRowOutput(
          warningsAndErrors = warningsAndErrors,
          recordNumber = row.getRecordNumber
        )
      } else {
        schema
          .map(s => {
            if (s.columns.length >= row.size()) {
              validateRow(row)
            } else {
              val raggedRowsError = ErrorWithCsvContext(
                "ragged_rows",
                "structure",
                row.getRecordNumber.toString,
                "",
                "",
                ""
              )
              val warningsAndErrors =
                WarningsAndErrors(errors = Array(raggedRowsError))
              ValidateRowOutput(
                warningsAndErrors = warningsAndErrors,
                recordNumber = row.getRecordNumber
              )
            }
          })
          .getOrElse(
            ValidateRowOutput(
              warningsAndErrors = WarningsAndErrors(),
              recordNumber = row.getRecordNumber
            )
          )
      }
    }
  }

  /**
    * Every PrimaryKey is hashed and stored in the hashMap - mapPrimaryKeyHashToRowNumbers along with the rowNumbers
    * Later on, the keys(which are the hashes) with more than one rowNumbers are checked again if they are actual primary
    * key violations or hash collisions.
    */
  private def accumulatePossiblePrimaryKeyDuplicates(
                                                      mapPrimaryKeyHashToRowNumbers: Map[Int, Array[Long]],
                                                      validateRowOutput: ValidateRowOutput
                                                    ): Map[Int, Array[Long]] = {
    val primaryKeyValueHash = validateRowOutput.primaryKeyValues.hashCode()
    if (validateRowOutput.primaryKeyValues.nonEmpty) {
      val existingRowsMatchingHash = mapPrimaryKeyHashToRowNumbers.getOrElse(
        primaryKeyValueHash,
        Array[Long]()
      )
      mapPrimaryKeyHashToRowNumbers.updated(
        primaryKeyValueHash,
        existingRowsMatchingHash :+ validateRowOutput.recordNumber
      )
    } else {
      mapPrimaryKeyHashToRowNumbers
    }
  }

  private def accumulateReferencedTableForeignKeyValues(
                                                         validateRowOutput: ValidateRowOutput,
                                                         mapReferencedTableToForeignKeyValues: MapForeignKeyReferenceToValues
                                                       ): MapForeignKeyReferenceToValues =
    validateRowOutput.parentTableForeignKeyReferences
      .foldLeft(mapReferencedTableToForeignKeyValues)({
        case (acc, (keyReference, keyValues)) =>
          val existingValues = acc.get(keyReference).getOrElse(Set())
          if (existingValues.contains(keyValues)) {
            acc.updated(
              keyReference,
              existingValues - keyValues + keyValues.copy(isDuplicate = true)
            )
          } else {
            acc.updated(keyReference, existingValues + keyValues)
          }
      })

  private def accumulateOriginTableForeignKeyValues(
                                                     validateRowOutput: ValidateRowOutput,
                                                     foreignKeyDefinitionsWithValues: MapForeignKeyDefinitionToValues
                                                   ): MapForeignKeyDefinitionToValues =
    validateRowOutput.childTableForeignKeys.foldLeft(
      foreignKeyDefinitionsWithValues
    ) {
      case (acc, (keyDefinition, keyValues)) =>
        val valuesForKey = acc
          .get(keyDefinition)
          .getOrElse(Set())
        acc.updated(keyDefinition, valuesForKey + keyValues)
    }


  private def validateRow(row: CSVRecord): ValidateRowOutput = {
    var errors = Array[ErrorWithCsvContext]()
    val primaryKeyValues = ArrayBuffer.empty[Any]
    val foreignKeyReferenceValues =
      ArrayBuffer.empty[
        (ReferencedTableForeignKeyReference, List[Any])
      ] // to store the validated referenced Table Columns values in each row
    val foreignKeyValues = {
      ArrayBuffer.empty[
        (ForeignKeyDefinition, List[Any])
      ] // to store the validated foreign key values in each row
    }

    schema.map(s => {
      for ((value, column) <- row.iterator.asScalaArray.zip(s.columns)) {
        //catch any exception here, possibly outOfBounds  and set warning too many values
        val (es, newValue) = column.validate(value)
        errors = errors ++ es.map(e =>
          ErrorWithCsvContext(
            e.`type`,
            "schema",
            row.getRecordNumber.toString,
            column.columnOrdinal.toString,
            e.content,
            s"required => ${column.required}"
          )
        )
        if (s.primaryKey.contains(column)) {
          primaryKeyValues.addAll(newValue)
        }

        for (foreignKeyReferenceObject <- foreignKeyReferences) {
          if (
            foreignKeyReferenceObject.referencedTableReferencedColumns.contains(
              column
            )
          ) {
            foreignKeyReferenceValues.addOne(
              (foreignKeyReferenceObject, newValue)
            )
          }
        }

        for (foreignKeyWrapperObject <- s.foreignKeys) {
          if (foreignKeyWrapperObject.localColumns.contains(column)) {
            foreignKeyValues.addOne((foreignKeyWrapperObject, newValue))
          }
        }
      }
    })

    ValidateRowOutput(
      row.getRecordNumber,
      WarningsAndErrors(Array(), errors),
      primaryKeyValues.toList,
      getForeignKeyReferencesWithPossibleValues(foreignKeyReferenceValues.toList, row),
      getForeignKeyDefinitionsWithValues(foreignKeyValues.toList, row)
    )
  }

  private def getForeignKeyDefinitionsWithValues(
                                   foreignKeyValues: List[(ForeignKeyDefinition, List[Any])],
                                   row: CSVRecord
  ): Predef.Map[ForeignKeyDefinition, KeyWithContext] = {
    foreignKeyValues
      .groupBy {
        case (k, _) => k
      }
      .map {
        case (k, values) =>
          (k, KeyWithContext(row.getRecordNumber, values.map(v => v._2)))
      }
  }

  private def getForeignKeyReferencesWithPossibleValues(
      foreignKeyReferenceValues: List[
        (ReferencedTableForeignKeyReference, List[Any])
      ],
      row: CSVRecord
  ): Predef.Map[ReferencedTableForeignKeyReference, KeyWithContext] = {
    foreignKeyReferenceValues
      .groupBy {
        case (k, _) => k
      }
      .map {
        case (k, values) =>
          (k, KeyWithContext(row.getRecordNumber, values.map(v => v._2)))
      }
  }

  def validateHeader(
      header: CSVRecord
  ): WarningsAndErrors = {
    var warnings: Array[WarningWithCsvContext] = Array()
    var errors: Array[ErrorWithCsvContext] = Array()
    var columnIndex = 0
    var columnNames: Array[String] = Array()
    while (columnIndex < header.size()) {
      val columnName = header.get(columnIndex).trim
      if (columnName == "") {
        warnings :+= WarningWithCsvContext(
          "Empty column name",
          "Schema",
          "",
          (columnIndex + 1).toString,
          "",
          ""
        )
      }
      if (columnNames.contains(columnName)) {
        warnings :+= WarningWithCsvContext(
          "Duplicate column name",
          "Schema",
          "",
          (columnIndex + 1).toString,
          columnName,
          ""
        )
      } else columnNames :+= columnName
      // Only validate columns are defined if a tableSchema has been defined.
      schema.map(s => {
        if (columnIndex < s.columns.length) {
          val column = s.columns(columnIndex)
          val WarningsAndErrors(w, e) = column.validateHeader(columnName)
          warnings = warnings.concat(w)
          errors = errors.concat(e)
        } else {
          errors :+= ErrorWithCsvContext(
            "Malformed header",
            "Schema",
            "1",
            "",
            "Unexpected column not defined in metadata",
            ""
          )
        }

      })
      columnIndex += 1
    }
    models.WarningsAndErrors(warnings, errors)
  }


  /**
    *
    * @param warnings
    * @param errors
    * @param mapForeignKeyDefinitionToKeys
    * @param mapForeignKeyReferenceToKeys
    * @param mapPrimaryKeyHashToRowNums - used to store the hashes of every primary keys and its row numbers.
    *   To validate primary key efficiently, we need to have them in memory and HashSets gave the best performance.
    *   Storing all of the primary keys in sets leads to huge memory usage by the application.
    *   To be memory efficient, we hash the primary keys and then store them along with the rowNumbers.
    *   By hashing primary key values we're only identifying possible duplicates and it can be an overestimate of
    *   the actual number of duplicates because of hash collisions. Hash Collisions are also addressed at later stage
    *
    *   {
    *     56234234234: {1, 20},
    *     45233453453: {2},
    *     234234234234: {345}
    *   }
    */
  case class AccumulatedTableKeyValues(
                                        warnings: Array[WarningWithCsvContext] = Array[WarningWithCsvContext](),
                                        errors: Array[ErrorWithCsvContext] = Array[ErrorWithCsvContext](),
                                        mapForeignKeyDefinitionToKeys: MapForeignKeyDefinitionToValues = Map(),
                                        mapForeignKeyReferenceToKeys: MapForeignKeyReferenceToValues = Map(),
                                        mapPrimaryKeyHashToRowNums: Map[Int, Array[Long]] = Map()
                                      )
}
