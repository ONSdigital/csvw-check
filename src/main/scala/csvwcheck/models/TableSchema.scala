package csvwcheck.models

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import csvwcheck.errors.{MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.InheritedProperties
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.{IterableHasAsScala, IteratorHasAsScala}

object TableSchema {
  def fromJson(standardisedTableSchema: ObjectNode): ParseResult[(Option[TableSchema], Array[WarningWithCsvContext])] =
    parseAndValidateColumns(standardisedTableSchema).flatMap(columns =>
      parseTableSchemaGivenColumns(
        standardisedTableSchema,
        columns
      )
    )

  private def parseAndValidateColumns(tableSchemaObject: ObjectNode): ParseResult[Array[Column]] =
    parseColumnDefinitions(tableSchemaObject)
      .flatMap(ensureNoDuplicateColumnNames)
      .flatMap(ensureVirtualColumnsAfterColumns)

  private def parseColumnDefinitions(tableSchemaObject: ObjectNode): ParseResult[Array[Column]] = {
    tableSchemaObject
      .getMaybeNode("columns")
      .map(columnsNode =>
        columnsNode
          .elements()
          .asScala
          .zipWithIndex
          .map(parseColumnDefinition(tableSchemaObject, _))
          .foldLeft[ParseResult[Array[Column]]](Right(Array()))({
            case (err@Left(_), _) => err
            case (_, Left(newError)) => Left(newError)
            case (Right(columns), Right(column)) => Right(columns :+ column)
          })
      )
      .getOrElse(Right(Array.empty))
  }

  private def parseColumnDefinition(
                                     tableSchemaObject: ObjectNode,
                                     colWithIndex: (JsonNode, Int)
                                   ): ParseResult[Column] =
    colWithIndex match {
      case (col, index) =>
        val columnObject = col.asInstanceOf[ObjectNode]
        val colNum = index + 1
        Column
          .fromJson(
            colNum,
            InheritedProperties.copyInheritedProperties(tableSchemaObject, columnObject)
          )
    }

  private def ensureNoDuplicateColumnNames(columns: Array[Column]): ParseResult[Array[Column]] = {
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
      Right(columns)
    }
  }

  private def ensureVirtualColumnsAfterColumns(columns: Array[Column]): ParseResult[Array[Column]] = {
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

    Right(columns)
  }

  private def parseTableSchemaGivenColumns(
                                            tableSchemaObject: ObjectNode,
                                            columns: Array[Column]
                                          ): ParseResult[(Option[TableSchema], Array[WarningWithCsvContext])] = {
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
            Some(tableSchema),
            pkWarnings
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
          arrayNode.asScala
            .map({
              case rowTitleElement: TextNode =>
                val rowTitle = rowTitleElement.asText
                columns
                  .find(col => col.name.contains(rowTitle))
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
              case (err@Left(_), _) => err
              case (_, Left(newErr)) => Left(newErr)
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
          foreignKeysNode.asScala
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
              case (err@Left(_), _) => err
              case (_, Left(newErr)) => Left(newErr)
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
    columnReferenceArrayNode.asScala
      .map({
        case columnReferenceElementTextNode: TextNode =>
          val columnReference = columnReferenceElementTextNode.asText
          columns
            .find(col => col.name.contains(columnReference))
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
        case (err@Left(_), _) => err
        case (_, Left(newErr)) => Left(newErr)
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
        primaryKeyNode.asScala
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
          .find(col => col.name.contains(primaryKeyColNameReference))
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
}

case class TableSchema private(
                                columns: Array[Column],
                                primaryKey: Array[Column],
                                rowTitleColumns: Array[Column],
                                foreignKeys: Array[ForeignKeyDefinition],
                                schemaId: Option[String]
                              )
