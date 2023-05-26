package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import csvwcheck.PropertyChecker.StringWarnings
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import csvwcheck.{PropertyChecker, models}
import org.apache.commons.csv.CSVRecord

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object Table {

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
          .map({
            case (parsedNode, stringWarnings, propertyType) =>
              (propertyName, parsedNode, stringWarnings, propertyType)
          })
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
        val tablePropertiesIncludingInherited =
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
          tablePropertiesIncludingInherited,
          baseUrl,
          lang
        ).flatMap(parseDialect(_, tablePropertiesIncludingInherited))
          .flatMap(parseNotes(_, tablePropertiesIncludingInherited))
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
            .map({
              case (pkColumns, pkWarnings) =>
                (rowTitleColumns, foreignKeyMappings, pkColumns, pkWarnings)
            })
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
          .find(col =>
            col.name.isDefined && col.name.get == primaryKeyColNameReference
          )
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

  case class PartitionedTableProperties(
      annotations: Map[String, JsonNode] = Map(),
      tableProperties: Map[String, JsonNode] = Map(),
      inheritedProperties: Map[String, JsonNode] = Map(),
      warnings: Array[WarningWithCsvContext] = Array()
  )
}

case class TableSchema(
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


  def validateRow(row: CSVRecord): ValidateRowOutput = {
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
      getParentTableForeignKeys(foreignKeyReferenceValues.toList, row),
      getChildForeignKeys(foreignKeyValues.toList, row)
    )
  }

  private def getChildForeignKeys(
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

  private def getParentTableForeignKeys(
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
}
