package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.helpers.MapHelpers
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.IteratorHasGetKeysAndValues
import csvwcheck.{PropertyChecker, models}
import org.apache.commons.csv.CSVRecord

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
object Table {

  def fromJson(
                tableDesc: ObjectNode,
                baseUrl: String,
                lang: String,
                commonProperties: mutable.Map[String, JsonNode],
                inheritedPropertiesIn: mutable.Map[String, JsonNode]
  ): (Table, Array[WarningWithCsvContext]) = {
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
        createTableForExistingSchema(
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
  ): (
      mutable.Map[String, JsonNode],
      mutable.Map[String, JsonNode],
      mutable.Map[String, JsonNode],
      Array[WarningWithCsvContext]
  ) = {
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

  private def createTableForExistingSchema(
                                            tableSchema: JsonNode,
                                            inheritedProperties: mutable.Map[String, JsonNode],
                                            baseUrl: String,
                                            lang: String,
                                            url: String,
                                            tableProperties: mutable.Map[String, JsonNode],
                                            annotations: mutable.Map[String, JsonNode],
                                            existingWarnings: Array[WarningWithCsvContext]
  ): (Table, Array[WarningWithCsvContext]) = {

    tableSchema match {
      case tableSchemaObject: ObjectNode =>
        var warnings = Array[WarningWithCsvContext]()
        warnings = warnings.concat(existingWarnings)

        ensureColumnsNodeIsArray(tableSchemaObject, url)
          .foreach(w => warnings :+= w)

        setTableSchemaInheritedProperties(
          inheritedProperties,
          tableSchemaObject
        )

        val (columns, columnWarnings) =
          validateAndExtractColumnsFromSchema(
            baseUrl,
            lang,
            inheritedProperties,
            tableSchemaObject
          )
        warnings = warnings.concat(columnWarnings)

        // Collect primary keys in primaryKeyColumns
        val (primaryKeyToReturn, primaryKeyWarnings) =
          collectPrimaryKeyColumns(tableSchemaObject, columns)
        warnings = warnings.concat(primaryKeyWarnings)

        // Collect foreign Keys in foreignKeyColumns
        val foreignKeyMappings =
          collectForeignKeyColumns(tableSchemaObject, columns)

        // Collect row titles column
        val rowTitlesColumns =
          collectRowTitlesColumns(tableSchemaObject, columns)

        val table = new Table(
          url = url,
          id = getId(tableProperties),
          schema = Some(
            TableSchema(
              columns = columns,
              foreignKeys = foreignKeyMappings, // a new type here?
              primaryKey = primaryKeyToReturn,
              rowTitleColumns = rowTitlesColumns,
              schemaId = getMaybeSchemaIdFromTableSchema(tableSchemaObject),
            )
          ),
          dialect = getDialect(tableProperties),
          notes = getNotes(tableProperties),
          suppressOutput = getSuppressOutput(tableProperties),
          annotations = annotations
        )
        (table, warnings)
      // tableSchemas defined in other files have been updated to inline representations by this point already.
      // See propertyChecker function for tableSchema.
      case _ =>
        throw MetadataError(
          s"Table schema must be object for table $url "
        )
    }
  }

  private def getDialect(
      tableProperties: mutable.Map[String, JsonNode]
  ): Option[Dialect] = {
    tableProperties
      .get("dialect")
      .flatMap {
        case d: ObjectNode => Some(Dialect.fromJson(d))
        case d if d.isNull => None
        case d =>
          throw MetadataError(
            s"Unexpected JsonNode type ${d.getClass.getName}"
          )
      }
  }

  private def ensureColumnsNodeIsArray(
      tableSchemaObject: ObjectNode,
      url: String
  ): Option[WarningWithCsvContext] = {
    val columnsNode = tableSchemaObject.path("columns")
    if (!columnsNode.isMissingNode && columnsNode.isArray) {
      None
    } else {
      tableSchemaObject.set("columns", JsonNodeFactory.instance.arrayNode())
      Some(
        WarningWithCsvContext(
          "invalid_value",
          "metadata",
          "",
          "",
          s"columns is not array for table: $url",
          ""
        )
      )
    }
  }

  private def validateAndExtractColumnsFromSchema(
                                                   baseUrl: String,
                                                   lang: String,
                                                   inheritedProperties: mutable.Map[String, JsonNode],
                                                   tableSchemaObject: ObjectNode
  ): (Array[Column], Array[WarningWithCsvContext]) = {
    val warnings = ArrayBuffer.empty[WarningWithCsvContext]

    val columnObjects = tableSchemaObject
      .get("columns")
      .elements()
      .asScalaArray

    val columns = columnObjects.zipWithIndex
      .flatMap {
        case (col, i) =>
          col match {
            case colObj: ObjectNode =>
              val colNum = i + 1
              val (colDef, w) = Column.fromJson(
                colNum,
                colObj,
                baseUrl,
                lang,
                inheritedProperties
              )
              warnings.addAll(
                w.map(e =>
                  WarningWithCsvContext(
                    e.`type`,
                    "metadata",
                    "",
                    colNum.toString,
                    e.content,
                    ""
                  )
                )
              )
              Some(colDef)
            case _ =>
              warnings.addOne(
                WarningWithCsvContext(
                  "invalid_column_description",
                  "metadata",
                  "",
                  "",
                  col.toString,
                  ""
                )
              )
              None
          }
      }

    val columnNames = columns.flatMap(c => c.name)

    ensureNoDuplicateColumnNames(columnNames)
    ensureVirtualColumnsAfterColumns(columns)
    (columns, warnings.toArray)
  }

  private def ensureNoDuplicateColumnNames(columnNames: Array[String]): Unit = {
    val duplicateColumnNames = columnNames
      .groupBy(identity)
      .filter { case (_, elements) => elements.length > 1 }
      .keys

    if (duplicateColumnNames.nonEmpty) {
      throw MetadataError(
        s"Multiple columns named ${duplicateColumnNames.mkString(", ")}"
      )
    }
  }

  private def ensureVirtualColumnsAfterColumns(columns: Array[Column]): Unit = {
    var virtualColumns = false
    for (column <- columns) {
      if (virtualColumns && !column.virtual) {
        throw MetadataError(
          s"virtual columns before non-virtual column ${column.name.get} (${column.columnOrdinal})"
        )
      }
      virtualColumns = virtualColumns || column.virtual
    }
  }

  private def collectRowTitlesColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): Array[Column] = {
    if (!tableSchemaObject.path("rowTitles").isMissingNode) {
      val rowTitlesColumns = ArrayBuffer.empty[Column]
      val rowTitles = tableSchemaObject.get("rowTitles")
      for (rowTitle <- rowTitles.elements().asScalaArray) {
        val maybeCol = columns.find(col =>
          col.name.isDefined && col.name.get == rowTitle.asText()
        )
        maybeCol match {
          case Some(col) => rowTitlesColumns.addOne(col)
          case None =>
            throw MetadataError(
              s"rowTitles references non-existant column - ${rowTitle.asText()}"
            )
        }
      }
      rowTitlesColumns.toArray
    } else {
      Array[Column]()
    }
  }

  private def collectForeignKeyColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): Array[ChildTableForeignKey] = {
    if (tableSchemaObject.path("foreignKeys").isMissingNode) {
      Array()
    } else {
      val foreignKeys = ArrayBuffer.empty[ChildTableForeignKey]
      val foreignKeysNode =
        tableSchemaObject.get("foreignKeys").asInstanceOf[ArrayNode]
      for (foreignKey <- foreignKeysNode.elements().asScalaArray) {
        val foreignKeyColumns = ArrayBuffer.empty[Column]
        for (
          reference <- foreignKey.get("columnReference").elements().asScalaArray
        ) {
          val maybeCol = columns.find(col =>
            col.name.isDefined && col.name.get == reference.asText()
          )
          maybeCol match {
            case Some(col) => foreignKeyColumns.addOne(col)
            case None =>
              throw MetadataError(
                s"foreignKey references non-existent column - ${reference.asText()}"
              )
          }
        }
        foreignKeys.addOne(
          ChildTableForeignKey(
            foreignKey.asInstanceOf[ObjectNode],
            foreignKeyColumns.toArray
          )
        )
      }
      foreignKeys.toArray
    }
  }

  private def collectPrimaryKeyColumns(
      tableSchemaObject: ObjectNode,
      columns: Array[Column]
  ): (Array[Column], Array[WarningWithCsvContext]) = {
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

  private def setTableSchemaInheritedProperties(
                                                 inheritedProperties: mutable.Map[String, JsonNode],
                                                 tableSchemaObject: ObjectNode
  ): Unit = {
    for ((property, value) <- tableSchemaObject.getKeysAndValues) {
      if (
        Array[String](
          "columns",
          "primaryKey",
          "foreignKeys",
          "rowTitles"
        ).contains(property)
      ) {
        inheritedProperties += (property -> value)
      }
      () // Ensure return type of for loop is consistent.
    }
  }

  private def getMaybeSchemaIdFromTableSchema(
      schema: ObjectNode
  ): Option[String] = {
    val idNode = schema.path("@id")
    if (idNode.isMissingNode) {
      None
    } else {
      Some(idNode.asText())
    }
  }

  private def getId(tableProperties: mutable.Map[String, JsonNode]): Option[String] = {
    val idNode = tableProperties.get("@id")
    idNode match {
      case Some(value) => Some(value.asText())
      case _           => None
    }
  }

  private def getNotes(
      tableProperties: mutable.Map[String, JsonNode]
  ): Option[ArrayNode] = {
    val notesNode = tableProperties.get("notes")
    notesNode match {
      case Some(value) =>
        value match {
          case a: ArrayNode => Some(a)
          case _            => throw MetadataError("Notes property should be an array")
        }
      case _ => None
    }
  }

  private def getSuppressOutput(
      tableProperties: mutable.Map[String, JsonNode]
  ): Boolean = {
    val suppressOutputNode = tableProperties.get("suppressOutput")
    suppressOutputNode match {
      case Some(value) => value.asBoolean()
      case _           => false
    }
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

case class TableSchema (
  columns: Array[Column],
  primaryKey: Array[Column],
  rowTitleColumns: Array[Column],
  foreignKeys: Array[ChildTableForeignKey],
  schemaId: Option[String],
)

case class Table private (
    url: String,
    id: Option[String],
    schema: Option[TableSchema],
    dialect: Option[Dialect],
    notes: Option[ArrayNode],
    suppressOutput: Boolean,
    annotations: mutable.Map[String, JsonNode]
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
