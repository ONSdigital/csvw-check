package csvwcheck.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode, TextNode}
import csvwcheck.PropertyChecker
import csvwcheck.enums.PropertyType
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.models.Table.{MapForeignKeyDefinitionToValues, MapForeignKeyReferenceToValues}
import csvwcheck.models.WarningsAndErrors.TolerableErrors
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.ObjectNodeExtentions.{IteratorHasGetKeysAndValues, ObjectNodeGetMaybeNode}
import shapeless.syntax.std.tuple.productTupleOps

import java.net.URL
import scala.math.sqrt
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
      .flatMap(_ => parseContext(tableGroupNodeIn, baseUrl, "und"))
      .flatMap({
        case (baseUrl, lang, warnings) =>
          partitionTableGroupProperties(
            tableGroupNode,
            baseUrl,
            lang
          ).map(props =>
            (baseUrl, lang, props.copy(warnings = props.warnings ++ warnings))
          )
      })
      .flatMap({
        case all @ (
              baseUrl,
              lang,
              PartitionedTableGroupProperties(_, common, inherited, _)
            ) =>
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
          linkForeignKeysToReferencedTables(
            baseUrl,
            tablesWithWarningsAndErrors.component
          ).map(tables =>
            (
              baseUrl,
              lang,
              parsedProperties,
              tablesWithWarningsAndErrors.copy(component = tables)
            )
          )
      })
      .map({
        case (
              baseUrl,
              _,
              PartitionedTableGroupProperties(annotations, common, _, warnings),
              tablesWithWarningsAndErrors
            ) =>
          val tableGroup = TableGroup(
            baseUrl,
            getId(common),
            tablesWithWarningsAndErrors.component,
            common.get("notes"),
            annotations
          )

          WithWarningsAndErrors(
            tableGroup,
            WarningsAndErrors(
              warnings =
                warnings ++ tablesWithWarningsAndErrors.warningsAndErrors.warnings,
              errors = tablesWithWarningsAndErrors.warningsAndErrors.errors
            )
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

  def parseContext(
      rootNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[(String, String, Array[WarningWithCsvContext])] = {
    (rootNode.get("@context") match {
      case a: ArrayNode => parseContextArrayNode(a, baseUrl, lang)
      case s: TextNode if s.asText == csvwContextUri =>
        Right((baseUrl, lang, Array[WarningWithCsvContext]()))
      case _ => Left(MetadataError("Invalid Context"))
    }).map(results => {
      rootNode.remove("@context")
      results
    })
  }

  // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#top-level-properties
  def parseContextArrayNode(
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
                parseContextObject(
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
    *
    * @param contextBaseAndLangObject - The context object.
    * @param baseUrl                  - The base URL of the CSV-W
    * @param lang                     - The language.
    * @return newBaseUrl, newLang, warnings (if any)
    */
  def parseContextObject(
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
                .parseJsonProperty(property, value, baseUrl, lang)
                .flatMap({
                  case (propertyNode, Array(), _) =>
                    val propertyTextValue = propertyNode.asText()
                    property match {
                      case "@base" => Right((propertyTextValue, lang, warnings))
                      case "@language" =>
                        Right((baseUrl, propertyTextValue, warnings))
                      case _ =>
                        Left(
                          MetadataError(
                            s"Unhandled context property '$property'"
                          )
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
                })
            case _ =>
              Left(
                MetadataError(
                  s"@context contains properties other than @base or @language $property)"
                )
              )
          }
      })
  }

  private def linkForeignKeysToReferencedTables(
      baseUrl: String,
      tables: Map[String, Table]
  ): ParseResult[Map[String, Table]] = {
    tables.foldLeft[ParseResult[Map[String, Table]]](Right(tables))({
      case (err @ Left(_), _) => err
      case (Right(tables), (_, originTable)) =>
        linkForeignKeysDefinedOnTable(baseUrl, tables, originTable)
    })
  }

  private def linkForeignKeysDefinedOnTable(
      baseUrl: String,
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
                baseUrl,
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
      baseUrl: String,
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
            baseUrl,
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
      baseUrl: String,
      tables: Map[String, Table],
      originTableUrl: String,
      foreignKeyArrayIndex: Int,
      referenceObject: ObjectNode
  ): ParseResult[Table] = {
    referenceObject
      .getMaybeNode("resource")
      .map(resourceNode => {
        val referencedTableUrl = new URL(
          new URL(baseUrl),
          resourceNode.asText()
        ).toString
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
            val schemaUrl =
              new URL(new URL(baseUrl), schemaReferenceNode.asText()).toString
            tables.values
              .filter(table =>
                table.schema.exists(s => s.schemaId.exists(_ == schemaUrl))
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
      .map({
        case columnReferenceNode: ArrayNode =>
          columnReferenceNode
            .elements()
            .asScalaArray
            .map({
              case columnReference: TextNode =>
                mapNameToColumn
                  .get(columnReference.asText())
                  .map(Right(_))
                  .getOrElse(
                    Left(
                      MetadataError(
                        s"column named ${columnReference
                          .asText()} does not exist in ${referencedTable.url}," +
                          s" $$.tables[?(@.url = '${definitionTable.url}')].tableSchema.foreign_keys[$foreignKeyOrdinal].reference.columnReference"
                      )
                    )
                  )
              case columnReferenceNode =>
                Left(
                  MetadataError(
                    s"Unexpected columnReference '${columnReferenceNode.toPrettyString}'"
                  )
                )
            })
            .foldLeft[ParseResult[Array[Column]]](Right(Array[Column]()))({
              case (err @ Left(_), _) => err
              case (_, Left(newErr))  => Left(newErr)
              case (Right(columns), Right(newColumn)) =>
                Right(columns :+ newColumn)
            })
        case columnReferenceNode =>
          Left(
            MetadataError(
              s"Unexpected columnReference node ${columnReferenceNode.toPrettyString}"
            )
          )
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

  private def parseTableGroupType(
      tableGroupNode: ObjectNode
  ): ParseResult[String] = {
    tableGroupNode
      .getMaybeNode("@type")
      .map(typeNode => {
        val allegedType = typeNode.asText
        if (allegedType == "TableGroup")
          Right(allegedType)
        else
          Left(
            MetadataError(
              s"@type of table group is not 'TableGroup', found @type to be a '$allegedType'"
            )
          )
      })
      .getOrElse(Right("TableGroup"))
  }

  def partitionTableGroupProperties(
      tableGroupNode: ObjectNode,
      baseUrl: String,
      lang: String
  ): ParseResult[PartitionedTableGroupProperties] = {
    tableGroupNode.getKeysAndValues
      .map({
        case (propertyName, valueNode)
            if validProperties.contains(propertyName) =>
          Right((propertyName, valueNode, Array[String](), PropertyType.Common))
        case (propertyName, valueNode) =>
          PropertyChecker
            .parseJsonProperty(propertyName, valueNode, baseUrl, lang)
            .map(propertyName +: _)
      })
      .foldLeft[ParseResult[PartitionedTableGroupProperties]](
        Right(PartitionedTableGroupProperties())
      )({
        case (err @ Left(_), _) => err
        case (_, Left(newErr))  => Left(newErr)
        case (
              Right(partitionedProperties),
              Right((propertyName, parsedValue, stringWarnings, propertyType))
            ) =>
          val warnings = partitionedProperties.warnings ++ stringWarnings.map(
            WarningWithCsvContext(
              _,
              "metadata",
              "",
              "",
              s"$propertyName : ${parsedValue.toPrettyString}",
              ""
            )
          )
          propertyType match {
            case PropertyType.Annotation =>
              Right(
                partitionedProperties.copy(
                  annotations =
                    partitionedProperties.annotations + (propertyName -> parsedValue),
                  warnings = warnings
                )
              )
            case PropertyType.Common =>
              Right(
                partitionedProperties.copy(
                  common =
                    partitionedProperties.common + (propertyName -> parsedValue),
                  warnings = warnings
                )
              )
            case PropertyType.Inherited =>
              Right(
                partitionedProperties.copy(
                  inherited =
                    partitionedProperties.inherited + (propertyName -> parsedValue),
                  warnings = warnings
                )
              )
            case _ =>
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
    tableGroupNode
      .getMaybeNode("tables")
      .map({
        case tablesArrayNode: ArrayNode if tablesArrayNode.isEmpty() =>
          Left(MetadataError("Empty tables property"))
        case tablesArrayNode: ArrayNode =>
          parseArrayNodeTables(
            tablesArrayNode,
            baseUrl,
            lang,
            commonProperties,
            inheritedProperties
          )
        case _ => Left(MetadataError("Tables property is not an array"))
      })
      .getOrElse(Left(MetadataError("No tables property")))
  }

  def parseArrayNodeTables(
      tablesArrayNode: ArrayNode,
      baseUrl: String,
      lang: String,
      commonProperties: Map[String, JsonNode],
      inheritedProperties: Map[String, JsonNode]
  ): ParseResult[WithWarningsAndErrors[Map[String, Table]]] = {
    tablesArrayNode
      .elements()
      .asScalaArray
      .foldLeft[ParseResult[WithWarningsAndErrors[Map[String, Table]]]](
        Right(WithWarningsAndErrors(Map(), WarningsAndErrors()))
      )({
        case (err @ Left(_), _) => err
        case (
              Right(WithWarningsAndErrors(tables, warningsAndErrors)),
              tableElementObject: ObjectNode
            ) =>
          val (tableUrl, errors) = parseTableUrl(tableElementObject)

          val tableUrlNode = new TextNode(
            new URL(new URL(baseUrl), tableUrl).toString
          )
          val modifiedTableNode = tableElementObject.deepCopy()
          modifiedTableNode.set("url", tableUrlNode)

          Table
            .fromJson(
              modifiedTableNode,
              baseUrl,
              lang,
              commonProperties,
              inheritedProperties
            )
            .map({
              case (table, warnings) =>
                WithWarningsAndErrors(
                  tables + (table.url -> table),
                  warningsAndErrors.copy(
                    warnings = warningsAndErrors.warnings ++ warnings,
                    errors = warningsAndErrors.errors ++ errors
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
              s"Table must be instance of object, found: ${tableElement}",
              ""
            )
          )
          Right(WithWarningsAndErrors(tables, newWarningsAndErrors))
      })
  }

  private def parseTableUrl(
      tableElementObject: ObjectNode
  ): (String, Array[ErrorWithCsvContext]) = {
    tableElementObject
      .getMaybeNode("url")
      .map({
        case urlTextNode: TextNode =>
          (urlTextNode.asText, Array[ErrorWithCsvContext]())
        case urlNode =>
          (
            "",
            Array(
              ErrorWithCsvContext(
                "invalid_url",
                "metadata",
                "",
                "",
                s"url: ${urlNode.toPrettyString}",
                ""
              )
            )
          )
      })
      .getOrElse(
        (
          "",
          Array(
            ErrorWithCsvContext(
              "invalid_url",
              "metadata",
              "",
              "",
              s"Did not find URL for table.",
              ""
            )
          )
        )
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
  type MapTableToForeignKeyDefinitions =
    Map[Table, MapForeignKeyDefinitionToValues]
  type MapTableToForeignKeyReferences =
    Map[Table, MapForeignKeyReferenceToValues]

  type TableState = (
    WarningsAndErrors,
      MapTableToForeignKeyDefinitions,
      MapTableToForeignKeyReferences
    )

  def validateCsvsAgainstTables(parallelism: Int, rowGrouping: Int): Source[WarningsAndErrors, NotUsed] = {
    val degreeOfParallelism = math.min(tables.size, sqrt(parallelism).floor.toInt)
    val degreeOfParallelismInTable = parallelism / degreeOfParallelism
    Source
      .fromIterator(() => tables.values.iterator)
      .flatMapMerge(
        degreeOfParallelism,
        table => table.parseCsv(degreeOfParallelismInTable, rowGrouping)
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
    } yield validateForeignKeyIntegrity(foreignKeyDefinitionsByTable, referencedTable, parentTableForeignKeyReference, allPossibleParentTableValues)

    errorsPerForeignKeyReference.flatMap(identity(_)).toArray
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
      .get(parentTableForeignKeyReference.definitionTable)
      .getOrElse(
        throw MetadataError(
          s"Could not find corresponding origin table(${parentTableForeignKeyReference.definitionTable.url}) for referenced table ${referencedTable.url}"
        )
      )
    foreignKeyDefinitionsOnTable
      .get(parentTableForeignKeyReference.foreignKeyDefinition)
      .getOrElse(
        throw MetadataError(
          s"Could not find foreign key against origin table." + parentTableForeignKeyReference.foreignKeyDefinition.jsonObject.toPrettyString
        )
      )
  }
}