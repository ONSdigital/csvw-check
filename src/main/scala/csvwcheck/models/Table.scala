package csvwcheck.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.typesafe.scalalogging.Logger
import csvwcheck.errors.{ErrorWithCsvContext, MetadataError, WarningWithCsvContext}
import csvwcheck.models
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.normalisation.InheritedProperties
import csvwcheck.normalisation.Utils.parseNodeAsText
import csvwcheck.traits.JavaIteratorExtensions.IteratorHasAsScalaArray
import csvwcheck.traits.LoggerExtensions.LogDebugException
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import shapeless.syntax.std.tuple.productTupleOps

import java.io.File
import java.net.URI
import java.nio.charset.Charset
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}
import scala.util.control.NonFatal

object Table {
  type MapForeignKeyDefinitionToValues =
    Map[ForeignKeyDefinition, Set[KeyWithContext]]
  type MapForeignKeyReferenceToValues =
    Map[ReferencedTableForeignKeyReference, Set[KeyWithContext]]
  type PrimaryKeysAndErrors =
    (mutable.Set[List[Any]], ArrayBuffer[ErrorWithCsvContext])

  def fromJson(standardisedTableObjectNode: ObjectNode, inheritedDialect: Option[Dialect] = None): ParseResult[(Table, Array[WarningWithCsvContext])] =
    getUrl(standardisedTableObjectNode)
      .flatMap(url =>
        standardisedTableObjectNode.getMaybeNode("tableSchema")
          .map(_.asInstanceOf[ObjectNode])
          .map(
            parseTable(
              standardisedTableObjectNode,
              _,
              url,
              inheritedDialect
            )
          )
          .getOrElse(Right((initializeTableWithDefaults(url, inheritedDialect), Array.empty)))
      )

  private def getUrl(standardisedTableObjectNode: ObjectNode): ParseResult[String] = {
    standardisedTableObjectNode
      .getMaybeNode("url")
      .map(parseNodeAsText(_))
      .getOrElse(Left(MetadataError("URL not found for table")))
  }

  private def parseTable(
                          tableNode: ObjectNode,
                          tableSchema: ObjectNode,
                          tableUrl: String,
                          inheritedDialect: Option[Dialect]
                        ): ParseResult[(Table, Array[WarningWithCsvContext])] = {
    val notes = tableNode
      .getMaybeNode("notes")
      .map(_.asInstanceOf[ArrayNode])

    val tableSchemaWithInheritedProperties = InheritedProperties.copyInheritedProperties(tableNode, tableSchema)
    for {
      schemaWithWarnings <- TableSchema.fromJson(tableSchemaWithInheritedProperties)
      dialect <- parseDialect(tableNode)
    } yield {
      val (tableSchema, warnings) = schemaWithWarnings
      val stuff = (
        new Table(
          dialect = dialect.orElse(inheritedDialect),
          url = tableUrl,
          id = tableNode.getMaybeNode("@id").map(_.asText()),
          notes = notes,
          suppressOutput = tableNode.getMaybeNode("suppressOutput").exists(_.asBoolean),
          schema = tableSchema
        ),
        warnings
      )
      stuff
    }
  }

  def parseDialect(tableOrGroupObjectNode: ObjectNode): ParseResult[Option[Dialect]] =
    tableOrGroupObjectNode
      .getMaybeNode("dialect")
      .map(_.asInstanceOf[ObjectNode])
      .map(
        Dialect
          .fromJson(_)
          .map(Some(_))
      )
      .getOrElse(Right(None))


  private def initializeTableWithDefaults(
                                           url: String,
                                           inheritedDialect: Option[Dialect]
                                         ): Table =
    new Table(
      url = url,
      id = None,
      schema = None,
      dialect = inheritedDialect,
      notes = None,
      suppressOutput = false
    )
}


case class Table private(
                          url: String,
                          suppressOutput: Boolean,
                          id: Option[String] = None,
                          schema: Option[TableSchema] = None,
                          dialect: Option[Dialect] = None,
                          notes: Option[ArrayNode] = None,
                          // This array contains the foreign keys defined in other tables' schemas which reference data inside this table.
                          foreignKeyReferences: Array[ReferencedTableForeignKeyReference] = Array()
                        ) {

  import csvwcheck.models.Table.{MapForeignKeyDefinitionToValues, MapForeignKeyReferenceToValues, PrimaryKeysAndErrors}

  val mapAvailableCharsets: mutable.Map[String, Charset] =
    Charset.availableCharsets().asScala
  private val logger = Logger(this.getClass.getName)

  override def toString: String = s"Table($url)"

  override def hashCode(): Int = url.hashCode

  implicit val ec: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.global

  override def equals(obj: Any): Boolean =
    obj match {
      case table: Table => table.url == this.url
      case _ => false
    }

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
        Source(
          List(
            (
              warningsAndErrors,
              Map[ForeignKeyDefinition, Set[KeyWithContext]](),
              Map[ReferencedTableForeignKeyReference, Set[KeyWithContext]]()
            )
          )
        )
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

  def validateHeader(
                      header: CSVRecord
                    ): WarningsAndErrors = {
    var warnings: Array[WarningWithCsvContext] = Array()
    var errors: Array[ErrorWithCsvContext] = Array()
    var columnIndex = 0
    var csvColumnTitles: Array[String] = Array()
    while (columnIndex < header.size()) {
      val csvColumnTitle = header.get(columnIndex).trim
      if (csvColumnTitle.isEmpty) {
        warnings :+= WarningWithCsvContext(
          "Empty column name",
          "Schema",
          "",
          (columnIndex + 1).toString,
          "",
          ""
        )
      }
      if (csvColumnTitles.contains(csvColumnTitle)) {
        warnings :+= WarningWithCsvContext(
          "Duplicate column name",
          "Schema",
          "",
          (columnIndex + 1).toString,
          csvColumnTitle,
          ""
        )
      } else csvColumnTitles :+= csvColumnTitle
      // Only validate columns are defined if a tableSchema has been defined.
      schema.foreach(s => {
        if (columnIndex < s.columns.length) {
          val column = s.columns(columnIndex)
          val WarningsAndErrors(w, e) = column.validateHeader(csvColumnTitle)
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
            throw new Exception(
              "Could not fetch CSV parser. This should never happen."
            )
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

  private def getParser(
                         dialect: Dialect,
                         format: CSVFormat
                       ): Either[WarningsAndErrors, CSVParser] = {
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
              Array(),
              Array(
                ErrorWithCsvContext(
                  "url_cannot_be_fetched",
                  "",
                  "",
                  "",
                  s"Url ${tableUri.toString} cannot be fetched",
                  ""
                )
              )
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
              .foreach(errorsInAkkaStreams.addOne)
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
          val existingValues = acc.getOrElse(keyReference, Set())
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
          .getOrElse(keyDefinition, Set())
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

    schema.foreach(s => {
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
      getForeignKeyReferencesWithPossibleValues(
        foreignKeyReferenceValues.toList,
        row
      ),
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

  /**
    *
    * @param warnings
    * @param errors
    * @param mapForeignKeyDefinitionToKeys
    * @param mapForeignKeyReferenceToKeys
    * @param mapPrimaryKeyHashToRowNums - used to store the hashes of every primary keys and its row numbers.
    *                                   To validate primary key efficiently, we need to have them in memory and HashSets gave the best performance.
    *                                   Storing all of the primary keys in sets leads to huge memory usage by the application.
    *                                   To be memory efficient, we hash the primary keys and then store them along with the rowNumbers.
    *                                   By hashing primary key values we're only identifying possible duplicates and it can be an overestimate of
    *                                   the actual number of duplicates because of hash collisions. Hash Collisions are also addressed at later stage
    *
    *                                   {
    *                                   56234234234: {1, 20},
    *                                   45233453453: {2},
    *                                   234234234234: {345}
    *                                   }
    */
  private case class AccumulatedTableKeyValues(
                                                warnings: Array[WarningWithCsvContext] = Array[WarningWithCsvContext](),
                                                errors: Array[ErrorWithCsvContext] = Array[ErrorWithCsvContext](),
                                                mapForeignKeyDefinitionToKeys: MapForeignKeyDefinitionToValues = Map(),
                                                mapForeignKeyReferenceToKeys: MapForeignKeyReferenceToValues = Map(),
                                                mapPrimaryKeyHashToRowNums: Map[Int, Array[Long]] = Map()
                                              )
}
