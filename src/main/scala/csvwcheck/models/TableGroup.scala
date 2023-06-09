package csvwcheck.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.models.Table.{MapForeignKeyDefinitionToValues, MapForeignKeyReferenceToValues, parseDialect}
import csvwcheck.models.WarningsAndErrors.TolerableErrors
import csvwcheck.normalisation.InheritedProperties
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.math.sqrt
import scala.util.matching.Regex

object TableGroup {
  val containsWhitespaces: Regex = ".*\\s.*".r

  def fromJson(
    standardisedTableGroupNode: ObjectNode
  ): ParseResult[WithWarningsAndErrors[TableGroup]] = {
    val (baseUrl, _) = parseContext(standardisedTableGroupNode)

    val warnings = if (containsWhitespaces.matches(baseUrl)) {
      Array(
        WarningWithCsvContext(
          "invalid_url",
          "metadata",
          "",
          "",
          "Base URL has whitespaces in it, please ensure its correctness. Proceeding with received path/url ..",
          ""
        )
      )
    } else {
      Array[WarningWithCsvContext]()
    }

    parseTables(standardisedTableGroupNode)
      .flatMap({
        case tablesWithWarningsAndErrors =>
          linkForeignKeysToReferencedTables(tablesWithWarningsAndErrors.component)
            .map(tables => tablesWithWarningsAndErrors.copy(component = tables))
      })
      .flatMap(tablesWithWarningsAndErrors => {
        for {
          dialect <- parseDialect(standardisedTableGroupNode)
        } yield {
          val tableGroup = TableGroup(
            baseUrl,
            getId(standardisedTableGroupNode),
            tablesWithWarningsAndErrors.component,
            standardisedTableGroupNode.getMaybeNode("notes"),
            dialect
          )

          WithWarningsAndErrors(
            tableGroup,
            WarningsAndErrors(
              warnings = warnings ++ tablesWithWarningsAndErrors.warningsAndErrors.warnings,
              errors = tablesWithWarningsAndErrors.warningsAndErrors.errors
            )
          )
        }
      })
  }

  def parseContext(
      tableGroupObjectNode: ObjectNode
  ): (String, String) = {
    val contextObject = tableGroupObjectNode
      .get("@context")
      .elements()
      .asScalaArray
      .apply(1)
      .asInstanceOf[ObjectNode]

    val baseUrl = contextObject.get("@base").asText
    val language = contextObject.get("@language").asText

    (baseUrl, language)
  }


  private def linkForeignKeysToReferencedTables(
      tables: Map[String, Table]
  ): ParseResult[Map[String, Table]] = {
    tables.foldLeft[ParseResult[Map[String, Table]]](Right(tables))({
      case (err @ Left(_), _) => err
      case (Right(tables), (_, originTable)) =>
        linkForeignKeysDefinedOnTable(tables, originTable)
    })
  }

  private def linkForeignKeysDefinedOnTable(
      tables: Map[String, Table],
      definitionTable: Table
  ): ParseResult[Map[String, Table]] = {
    definitionTable.schema
      .map(tableSchema =>
        tableSchema.foreignKeys.zipWithIndex
          .foldLeft[ParseResult[Map[String, Table]]](Right(tables))({
            case (err @ Left(_), _) => err
            case (Right(tables), (foreignKey, foreignKeyOrdinal)) =>
              linkForeignKeyToReferencedTable(
                tables,
                definitionTable,
                foreignKey,
                foreignKeyOrdinal
              )
          })
      )
      .getOrElse(Right(tables))
  }

  private def linkForeignKeyToReferencedTable(
      tables: Map[String, Table],
      definitionTable: Table,
      foreignKey: ForeignKeyDefinition,
      foreignKeyOrdinal: Int
  ): ParseResult[Map[String, Table]] = {
    foreignKey.jsonObject
      .getMaybeNode("reference")
      .map({
        case referenceObjectNode: ObjectNode =>
          getReferencedTableForForeignKey(
            tables,
            definitionTable.url,
            foreignKeyOrdinal,
            referenceObjectNode
          ).flatMap(referencedTable =>
              referencedTable.schema
                .map(
                  setForeignKeyOnReferencedTable(
                    definitionTable,
                    foreignKey,
                    referencedTable,
                    _,
                    referenceObjectNode,
                    foreignKeyOrdinal
                  )
                )
                .getOrElse(
                  Left(
                    MetadataError(
                      s"Unable to locate schema for table '${definitionTable.url}'"
                    )
                  )
                )
            )
            .map(referencedTable =>
              tables.updated(referencedTable.url, referencedTable)
            )
        case referenceNode =>
          Left(
            MetadataError(
              s"Foreign Key reference was not an object: ${referenceNode.toPrettyString}"
            )
          )
      })
      .getOrElse(
        Left(
          MetadataError(
            s"Foreign key reference node unset on '${definitionTable.url}' foreign key at index $foreignKeyOrdinal."
          )
        )
      )
  }

  private def getReferencedTableForForeignKey(
      tables: Map[String, Table],
      originTableUrl: String,
      foreignKeyArrayIndex: Int,
      referenceObject: ObjectNode
  ): ParseResult[Table] = {
    referenceObject
      .getMaybeNode("resource")
      .map(resourceNode => {
        val referencedTableUrl = resourceNode.asText
        tables
          .get(referencedTableUrl)
          .map(Right(_))
          .getOrElse(
            Left(
              MetadataError(
                s"Could not find foreign key referenced table $referencedTableUrl, " +
                  s"$$.tables[?(@.url = '$originTableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.resource"
              )
            )
          )
      })
      .getOrElse(
        referenceObject
          .getMaybeNode("schemaReference")
          .map(schemaReferenceNode => {
            val schemaUrl = schemaReferenceNode.asText
            tables.values
              .filter(table =>
                table.schema.exists(s => s.schemaId.contains(schemaUrl))
              )
              .toList
              .headOption
              .map(Right(_))
              .getOrElse(
                Left(
                  MetadataError(
                    s"Could not find foreign key referenced schema $schemaUrl, " +
                      s"$$.tables[?(@.url = '$originTableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference.SchemaReference"
                  )
                )
              )
          })
          .getOrElse(
            Left(
              MetadataError(
                s"Could not find foreign `resource` or `schemaReference` on " +
                  s"$$.tables[?(@.url = '$originTableUrl')].tableSchema.foreignKeys[$foreignKeyArrayIndex].reference"
              )
            )
          )
      )
  }

  private def setForeignKeyOnReferencedTable(
      definitionTable: Table,
      foreignKeyDefinition: ForeignKeyDefinition,
      referencedTable: Table,
      referencedTableSchema: TableSchema,
      referenceNode: ObjectNode,
      foreignKeyOrdinal: Int
  ): ParseResult[Table] = {
    val mapNameToColumn = referencedTableSchema.columns
      .flatMap(col => col.name.map((_, col)))
      .toMap

    referenceNode
      .getMaybeNode("columnReference")
      .map(columnReferenceNode => {
        columnReferenceNode
          .elements()
          .asScalaArray
          .map(columnReference => {
            mapNameToColumn
              .get(columnReference.asText)
              .map(Right(_))
              .getOrElse(
                Left(
                  MetadataError(
                    s"column named ${
                      columnReference
                        .asText()
                    } does not exist in ${referencedTable.url}," +
                      s" $$.tables[?(@.url = '${definitionTable.url}')].tableSchema.foreign_keys[$foreignKeyOrdinal].reference.columnReference"
                  )
                )
              )
          })
          .foldLeft[ParseResult[Array[Column]]](Right(Array[Column]()))({
            case (err@Left(_), _) => err
            case (_, Left(newErr)) => Left(newErr)
            case (Right(columns), Right(newColumn)) =>
              Right(columns :+ newColumn)
          })
      })
      .getOrElse(
        Left(MetadataError("Did not find columnReference node."))
      )
      .map(referencedTableColumns =>
        referencedTable.copy(
          foreignKeyReferences =
            referencedTable.foreignKeyReferences :+ ReferencedTableForeignKeyReference(
              foreignKeyDefinition,
              referencedTable,
              referencedTableColumns,
              definitionTable
            )
        )
      )
  }

  private def parseTables(tableGroupNode: ObjectNode): ParseResult[WithWarningsAndErrors[Map[String, Table]]] = {
    tableGroupNode.get("tables")
      .elements()
      .asScalaArray
      .foldLeft[ParseResult[WithWarningsAndErrors[Map[String, Table]]]](
        Right(WithWarningsAndErrors(Map(), WarningsAndErrors()))
      )({
        case (err @ Left(_), _) => err
        case (
          Right(WithWarningsAndErrors(tables, warningsAndErrors)),
          tableObjectNode: ObjectNode
          ) =>
          Table
            .fromJson(InheritedProperties.copyInheritedProperties(tableGroupNode, tableObjectNode))
            .map({
              case (table, warnings) =>
                WithWarningsAndErrors(
                  tables + (table.url -> table),
                  warningsAndErrors.copy(
                    warnings = warningsAndErrors.warnings ++ warnings
                  )
                )
            })
        case (
          Right(WithWarningsAndErrors(tables, warningsAndErrors)),
          tableElement
          ) =>
          val newWarningsAndErrors = warningsAndErrors.copy(warnings =
            warningsAndErrors.warnings :+ WarningWithCsvContext(
              "invalid_table_description",
              "metadata",
              "",
              "",
              s"Table must be instance of object, found: $tableElement",
              ""
            )
          )
          Right(WithWarningsAndErrors(tables, newWarningsAndErrors))
      })
  }

  private def getId(tableGroupNode: ObjectNode): Option[String] = {
    tableGroupNode.getMaybeNode("@id")
      .map(_.asText)
  }
}

case class TableGroup private (
    baseUrl: String,
    id: Option[String],
    tables: Map[String, Table],
    notes: Option[JsonNode],
    dialect: Option[Dialect]
) {
  type MapTableToForeignKeyDefinitions =
    Map[Table, MapForeignKeyDefinitionToValues]
  type MapTableToForeignKeyReferences =
    Map[Table, MapForeignKeyReferenceToValues]

  type TableState = (
      WarningsAndErrors,
      MapTableToForeignKeyDefinitions,
      MapTableToForeignKeyReferences
  )

  def validateCsvsAgainstTables(
      parallelism: Int,
      rowGrouping: Int
  ): Source[WarningsAndErrors, NotUsed] = {
    val degreeOfParallelism =
      math.min(tables.size, sqrt(parallelism).floor.toInt)
    val degreeOfParallelismInTable = parallelism / degreeOfParallelism
    Source
      .fromIterator(() => tables.values.iterator)
      .flatMapMerge(
        degreeOfParallelism,
        table =>
          table
            .parseCsv(degreeOfParallelismInTable, rowGrouping)
            .map(_ :+ table)
      )
      .fold[TableState](
        (
          WarningsAndErrors(),
          Map(),
          Map()
        )
      ) {
        case (
              (
                warningsAndErrorsAccumulator,
                foreignKeysAccumulator,
                foreignKeyReferencesAccumulator
              ),
              (
                warningsAndErrorsSource,
                foreignKeysSource,
                foreignKeyReferencesSource,
                table
              )
            ) =>
          (
            WarningsAndErrors(
              warningsAndErrorsAccumulator.warnings ++ warningsAndErrorsSource.warnings,
              warningsAndErrorsAccumulator.errors ++ warningsAndErrorsSource.errors
            ),
            foreignKeysAccumulator.updated(table, foreignKeysSource),
            foreignKeyReferencesAccumulator.updated(
              table,
              foreignKeyReferencesSource
            )
          )
      }
      .map {
        case (
              warningsAndErrors,
              allForeignKeyDefinitions,
              allForeignKeyReferences
            ) =>
          WarningsAndErrors(
            errors = warningsAndErrors.errors ++ validateForeignKeyIntegrity(
              allForeignKeyDefinitions,
              allForeignKeyReferences
            ),
            warnings = warningsAndErrors.warnings
          )
      }
  }

  private def validateForeignKeyIntegrity(
      foreignKeyDefinitionsByTable: MapTableToForeignKeyDefinitions,
      foreignKeyReferencesByTable: MapTableToForeignKeyReferences
  ): TolerableErrors = {
    // Origin/Definition Table : Referenced Table
    // Country, Year, Population  : Country, Name
    // UK, 2021, 67M  : UK, United Kingdom
    // EU, 2021, 448M : EU, Europe
    val errorsPerForeignKeyReference = for {
      (referencedTable, mapForeignKeyReferenceToAllPossibleValues) <-
        foreignKeyReferencesByTable

      (parentTableForeignKeyReference, allPossibleParentTableValues) <-
        mapForeignKeyReferenceToAllPossibleValues
    } yield validateForeignKeyIntegrity(
      foreignKeyDefinitionsByTable,
      referencedTable,
      parentTableForeignKeyReference,
      allPossibleParentTableValues
    )

    errorsPerForeignKeyReference.flatten.toArray
  }

  private def validateForeignKeyIntegrity(
      foreignKeyDefinitionsByTable: MapTableToForeignKeyDefinitions,
      referencedTable: Table,
      foreignKeyReference: ReferencedTableForeignKeyReference,
      allValidValues: Set[KeyWithContext]
  ): TolerableErrors = {
    val keysReferencesInOriginTable = getKeysReferencedInOriginTable(
      foreignKeyDefinitionsByTable,
      referencedTable,
      foreignKeyReference
    )

    getUnmatchedForeignKeyReferences(
      allValidValues,
      keysReferencesInOriginTable
    ) ++
      getDuplicateKeysInReferencedTable(
        allValidValues,
        keysReferencesInOriginTable
      )
  }
  private def getDuplicateKeysInReferencedTable(
      allPossibleParentTableValues: Set[KeyWithContext],
      keysReferencesInOriginTable: Set[KeyWithContext]
  ): TolerableErrors = {
    val duplicateKeysInParent = allPossibleParentTableValues
      .intersect(keysReferencesInOriginTable)
      .filter(k => k.isDuplicate)

    if (duplicateKeysInParent.nonEmpty) {
      duplicateKeysInParent
        .map(k =>
          ErrorWithCsvContext(
            "multiple_matched_rows",
            "schema",
            k.rowNumber.toString,
            "",
            k.keyValuesToString(),
            ""
          )
        )
        .toArray
    } else {
      Array.empty
    }
  }

  private def getUnmatchedForeignKeyReferences(
      allPossibleParentTableValues: Set[KeyWithContext],
      keysReferencesInOriginTable: Set[KeyWithContext]
  ): TolerableErrors = {
    val keyValuesNotDefinedInParent =
      keysReferencesInOriginTable.diff(allPossibleParentTableValues)
    if (keyValuesNotDefinedInParent.nonEmpty) {
      keyValuesNotDefinedInParent
        .map(k =>
          ErrorWithCsvContext(
            "unmatched_foreign_key_reference",
            "schema",
            k.rowNumber.toString,
            "",
            k.keyValuesToString(),
            ""
          )
        )
        .toArray
    } else {
      Array.empty
    }
  }

  private def getKeysReferencedInOriginTable(
      foreignKeyDefinitionsByTable: MapTableToForeignKeyDefinitions,
      referencedTable: Table,
      parentTableForeignKeyReference: ReferencedTableForeignKeyReference
  ): Set[KeyWithContext] = {
    val foreignKeyDefinitionsOnTable = foreignKeyDefinitionsByTable
      .getOrElse(
        parentTableForeignKeyReference.definitionTable,
        throw MetadataError(
          s"Could not find corresponding origin table(${parentTableForeignKeyReference.definitionTable.url}) for referenced table ${referencedTable.url}"
        )
      )
    foreignKeyDefinitionsOnTable
      .getOrElse(
        parentTableForeignKeyReference.foreignKeyDefinition,
        throw MetadataError(
          s"Could not find foreign key against origin table." + parentTableForeignKeyReference.foreignKeyDefinition.jsonObject.toPrettyString
        )
      )
  }
}
