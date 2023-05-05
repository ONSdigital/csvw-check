package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{
  ErrorWithCsvContext,
  MetadataError,
  WarningWithCsvContext
}
import csvwcheck.helpers.MapHelpers
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.{
  IteratorHasGetKeysAndValues,
  ObjectNodeGetMaybeNode
}
import csvwcheck.{PropertyChecker, models}
import org.apache.commons.csv.CSVRecord

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
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
      commonProperties: mutable.Map[String, JsonNode],
      inheritedPropertiesIn: mutable.Map[String, JsonNode]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    val (annotations, tableProperties, inheritedProperties, warnings) =
      partitionAndValidateTablePropertiesByType(
        commonProperties,
        inheritedPropertiesIn,
        tableDesc,
        baseUrl,
        lang
      )

    val url: String = getUrlEnsureExists(tableProperties)

    val maybeTableSchema: Option[JsonNode] =
      extractTableSchema(tableProperties, inheritedProperties)

    maybeTableSchema match {
      case Some(tableSchema) =>
        parseTableSchema(
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
        initializeTableWithDefaults(
          annotations,
          warnings,
          url
        )
    }
  }

  private def partitionAndValidateTablePropertiesByType(
      commonProperties: mutable.Map[String, JsonNode],
      inheritedProperties: mutable.Map[String, JsonNode],
      tableDesc: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[
    (
        mutable.Map[String, JsonNode],
        mutable.Map[String, JsonNode],
        mutable.Map[String, JsonNode],
        Array[WarningWithCsvContext]
    )
  ] = {
    val warnings = ArrayBuffer.empty[WarningWithCsvContext]
    val annotations = mutable.Map[String, JsonNode]()
    val tableProperties: mutable.Map[String, JsonNode] =
      MapHelpers.deepCloneJsonPropertiesMap(commonProperties)
    val inheritedPropertiesCopy: mutable.Map[String, JsonNode] =
      MapHelpers.deepCloneJsonPropertiesMap(inheritedProperties)
    for ((property, value) <- tableDesc.getKeysAndValues) {
      (property, value) match {
        case ("@type", s: TextNode) if s.asText == "Table" =>
        case ("@type", _: TextNode) =>
          throw MetadataError(
            s"@type of table is not 'Table' - ${tableDesc.get("url").asText()}.@type"
          )
        case ("@type", v) =>
          throw MetadataError(
            s"Unexpected value for '@type'. Expected string but got ${v.getNodeType} (${v.toPrettyString})"
          )
        case _ =>
          val (newValue, w, csvwPropertyType) =
            PropertyChecker.parseJsonProperty(property, value, baseUrl, lang)
          warnings.addAll(
            w.map(x =>
              WarningWithCsvContext(
                x,
                "metadata",
                "",
                "",
                s"$property : $value",
                ""
              )
            )
          )
          csvwPropertyType match {
            case PropertyType.Annotation =>
              annotations += (property -> newValue)
            case PropertyType.Table | PropertyType.Common =>
              tableProperties += (property -> newValue)
            case PropertyType.Column =>
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
            case _ => inheritedPropertiesCopy += (property -> newValue)
          }
      }
    }
    (annotations, tableProperties, inheritedPropertiesCopy, warnings.toArray)
  }

  private def getUrlEnsureExists(
      tableProperties: mutable.Map[String, JsonNode]
  ): String = {
    tableProperties
      .getOrElse("url", throw MetadataError("URL not found for table"))
      .asText()
  }

  private def extractTableSchema(
      tableProperties: mutable.Map[String, JsonNode],
      inheritedPropertiesCopy: mutable.Map[String, JsonNode]
  ): Option[JsonNode] = {
    tableProperties
      .get("tableSchema")
      .orElse(inheritedPropertiesCopy.get("tableSchema"))
  }

  private def parseTableSchema(
      tableSchema: JsonNode,
      inheritedProperties: mutable.Map[String, JsonNode],
      baseUrl: String,
      lang: String,
      tableUrl: String,
      tableProperties: mutable.Map[String, JsonNode],
      annotations: mutable.Map[String, JsonNode],
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
          collectPrimaryKeyColumns(tableSchemaObject, columns)
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
  ): ParseResult[Array[ChildTableForeignKey]] = {
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
            .foldLeft[ParseResult[Array[ChildTableForeignKey]]](Right(Array()))({
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
  ): ParseResult[ChildTableForeignKey] = {
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
  ): ParseResult[ChildTableForeignKey] = {
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
        ChildTableForeignKey(foreignKeyObjectNode, foreignKeyColumns)
      )
  }

  private def collectPrimaryKeyColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): ParseResult[(Array[Column], Array[WarningWithCsvContext])] = {
    // todo: This needs to be altered to return a ParseResult (and return Left(MetadataError) in the right places.
    val warnings = ArrayBuffer.empty[WarningWithCsvContext]
    if (!tableSchemaObject.path("primaryKey").isMissingNode) {
      var primaryKeyColumns = Array[Column]()
      val primaryKeys = tableSchemaObject.get("primaryKey")
      var primaryKeyValid = true
      for (reference <- primaryKeys.elements().asScalaArray) {
        val maybeCol = columns.find(col =>
          col.name.isDefined && col.name.get == reference.asText()
        )
        maybeCol match {
          case Some(col) => primaryKeyColumns :+= col
          case None =>
            warnings.addOne(
              WarningWithCsvContext(
                "invalid_column_reference",
                "metadata",
                "",
                "",
                s"primaryKey: $reference",
                ""
              )
            )
            primaryKeyValid = false

        }
      }
      if (primaryKeyValid && primaryKeyColumns.nonEmpty)
        return (primaryKeyColumns, warnings.toArray)
    }
    (Array[Column](), warnings.toArray)
  }

  private def getTableSchemaInheritedProperties(
      inheritedProperties: mutable.Map[String, JsonNode],
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
      tableProperties: mutable.Map[String, JsonNode]
  ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    val (table, warnings) = tableAndWarnings

    tableProperties
      .get("notes")
      .map({
        case notesNode: ArrayNode =>
          Right(table.copy(notes = Some(notesNode)), warnings)
        case _ => Right(tableAndWarnings)
      })
      .getOrElse(Left(MetadataError("Notes property should be an array")))
  }

  private def initializeTableWithDefaults(
      annotations: mutable.Map[String, JsonNode],
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
}

case class TableSchema(
    columns: Array[Column],
    primaryKey: Array[Column],
    rowTitleColumns: Array[Column],
    foreignKeys: Array[ChildTableForeignKey],
    schemaId: Option[String]
)

case class Table private (
    url: String,
    suppressOutput: Boolean,
    annotations: mutable.Map[String, JsonNode],
    id: Option[String] = None,
    schema: Option[TableSchema] = None,
    dialect: Option[Dialect] = None,
    notes: Option[ArrayNode] = None
) {

  /**
    * This array contains the foreign keys defined in other tables' schemas which reference data inside this table.
    */
  var foreignKeyReferences: Array[ParentTableForeignKeyReference] = Array()

  def validateRow(row: CSVRecord): ValidateRowOutput = {
    var errors = Array[ErrorWithCsvContext]()
    val primaryKeyValues = ArrayBuffer.empty[Any]
    val foreignKeyReferenceValues =
      ArrayBuffer.empty[
        (ParentTableForeignKeyReference, List[Any])
      ] // to store the validated referenced Table Columns values in each row
    val foreignKeyValues = {
      ArrayBuffer.empty[
        (ChildTableForeignKey, List[Any])
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
            foreignKeyReferenceObject.parentTableReferencedColumns.contains(
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
      foreignKeyValues: List[(ChildTableForeignKey, List[Any])],
      row: CSVRecord
  ): Predef.Map[ChildTableForeignKey, KeyWithContext] = {
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
        (ParentTableForeignKeyReference, List[Any])
      ],
      row: CSVRecord
  ): Predef.Map[ParentTableForeignKeyReference, KeyWithContext] = {
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
