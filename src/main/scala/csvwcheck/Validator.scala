package csvwcheck

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
import com.typesafe.scalalogging.Logger
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.errors._
import csvwcheck.models.WarningsAndErrors.Errors
import csvwcheck.models._
import csvwcheck.traits.LoggerExtensions.LogDebugException
import csvwcheck.traits.OptionExtensions.OptionIfDefined
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import sttp.client3.{HttpClientSyncBackend, Identity, SttpBackend, basicRequest}
import sttp.model.Uri

import java.io.{File, IOException}
import java.net.URI
import java.nio.charset.Charset
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{IterableHasAsScala, MapHasAsScala}
import scala.language.postfixOps
import scala.math.sqrt
import scala.util.control.NonFatal

class Validator(
    val schemaUri: Option[String],
    csvUri: Option[String] = None,
    httpClient: SttpBackend[Identity, Any] = HttpClientSyncBackend()
) {
  type ForeignKeys =
    mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]]
  type ForeignKeyReferences =
    mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[KeyWithContext]]
  type MapTableToForeignKeys = mutable.Map[Table, ForeignKeys]
  type MapTableToForeignKeyReferences = mutable.Map[Table, ForeignKeyReferences]
  type TableState =
    (WarningsAndErrors, MapTableToForeignKeys, MapTableToForeignKeyReferences)
  type TableStateWithPrimaryKeyHashes = (
      ArrayBuffer[WarningWithCsvContext], // Collection of warnings
      ArrayBuffer[ErrorWithCsvContext], // Collection of errors
      mutable.Map[ForeignKeyDefinition, mutable.Set[
        KeyWithContext
      ]], // Map of childTableForeignKey to Set[KeyWithContext]
      mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[
        KeyWithContext
      ]], // Map of parentTableForeignKey reference
      // to Set[KeyWithContext]
      mutable.Map[Int, ArrayBuffer[
        Long
      ]] // Map of primaryKey hash to row numbers
  )
  type PrimaryKeysAndErrors =
    (mutable.Set[List[Any]], ArrayBuffer[ErrorWithCsvContext])
  val mapAvailableCharsets: mutable.Map[String, Charset] =
    Charset.availableCharsets().asScala
  val parallelism: Int = sys.env.get("PARALLELISM") match {
    case Some(value) => value.toInt
    case None        => Runtime.getRuntime.availableProcessors()
  }
  private val logger = Logger(this.getClass.getName)
  private var sourceUriUsed: Boolean = false

  def validate(): Source[WarningsAndErrors, NotUsed] = {
    val absoluteSchemaUri = schemaUri.map(getAbsoluteSchemaUri)

    val maybeCsvUri = csvUri.map(new URI(_))

    val schemaUrisToCheck = maybeCsvUri
      .map(csvUri =>
        Array(
          absoluteSchemaUri,
          Some(new URI(s"${getUriWithoutQueryString(csvUri)}-metadata.json")),
          Some(csvUri.resolve("csv-metadata.json"))
        )
      )
      .getOrElse(Array(absoluteSchemaUri))
      .flatten
      .distinct

    findAndValidateCsvwSchemaFileForCsv(maybeCsvUri, schemaUrisToCheck.toSeq)
  }

  def validateSchemaTables(
      schema: TableGroup
  ): Source[WarningsAndErrors, NotUsed] = {
    val degreeOfParallelism =
      math.min(schema.tables.size, sqrt(parallelism).floor.toInt)
    val degreeOfParallelismInTable = parallelism / degreeOfParallelism
    Source
      .fromIterator(() => schema.tables.keys.iterator)
      .flatMapMerge(
        degreeOfParallelism,
        tableUrl => {
          val table = schema.tables(tableUrl)
          val tableUri = new URI(tableUrl)
          val dialect = table.dialect.getOrElse(Dialect())
          val format = getCsvFormat(dialect)

          readAndValidateCsv(
            schema,
            tableUri,
            format,
            dialect,
            degreeOfParallelismInTable
          ).map {
            case (warningsAndErrors, foreignKeys, foreignKeyReferences) =>
              (warningsAndErrors, foreignKeys, foreignKeyReferences, table)
          }
        }
      )
      .fold[TableState](
        (
          WarningsAndErrors(),
          mutable.Map(),
          mutable.Map()
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
          val wAndE = WarningsAndErrors(
            warningsAndErrorsAccumulator.warnings ++ warningsAndErrorsSource.warnings,
            warningsAndErrorsAccumulator.errors ++ warningsAndErrorsSource.errors
          )
          foreignKeysAccumulator(table) = foreignKeysSource
          foreignKeyReferencesAccumulator(table) = foreignKeyReferencesSource
          (
            wAndE,
            foreignKeysAccumulator,
            foreignKeyReferencesAccumulator
          )
      }
      .map {
        case (warningsAndErrors, allForeignKeys, allForeignKeyReferences) =>
          val foreignKeyErrors = validateForeignKeyReferences(
            allForeignKeys,
            allForeignKeyReferences
          )
          WarningsAndErrors(
            errors = warningsAndErrors.errors ++ foreignKeyErrors,
            warnings = warningsAndErrors.warnings
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

  def getParser(
      tableUri: URI,
      dialect: Dialect,
      format: CSVFormat
  ): Either[WarningsAndErrors, CSVParser] = {
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

  def readAndValidateCsv(
      schema: TableGroup,
      tableUri: URI,
      format: CSVFormat,
      dialect: Dialect,
      degreeOfParallelismInTable: Int
  ): Source[
    (
        WarningsAndErrors,
        ForeignKeys,
        ForeignKeyReferences
    ),
    NotUsed
  ] = {
    getParser(tableUri, dialect, format) match {
      case Right(parser) =>
        readAndValidateWithParser(
          schema,
          tableUri,
          dialect,
          format,
          parser,
          degreeOfParallelismInTable
        ).recover {
          case NonFatal(err) =>
            logger.debug(err)
            val warnings = Array(
              WarningWithCsvContext(
                "source_url_mismatch",
                "CSV requested not found in metadata",
                "",
                "",
                s"Schema: ${schema.baseUrl}, tableUri: $tableUri",
                ""
              )
            )

            (
              models.WarningsAndErrors(warnings = warnings),
              mutable.Map(),
              mutable.Map()
            )
        }
      case Left(warningsAndErrors) =>
        if (tableUri.toString != csvUri.get && !sourceUriUsed) {
          sourceUriUsed = true
          readAndValidateCsv(
            schema,
            new URI(csvUri.get),
            format,
            dialect,
            degreeOfParallelismInTable
          )
        }
        // AND tableUri != sourceUri
        // THEN we re-try readAndValidate using the sourceUri as the tableUri
        // AND THEN we set a flag on Validator to say we had to use the sourceUri

        val collection = List(
          (
            warningsAndErrors,
            mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]](),
            mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[
              KeyWithContext
            ]]()
          )
        )
        Source(collection)
    }
  }

  private def getAbsoluteSchemaUri(schemaPath: String): URI = {
    val inputSchemaUri = new URI(schemaPath)
    if (inputSchemaUri.getScheme == null) {
      new URI(s"file://${new File(schemaPath).getAbsolutePath}")
    } else {
      inputSchemaUri
    }
  }

  private def attemptToFindMatchingTableGroup(
      maybeCsvUri: Option[URI],
      possibleSchemaUri: URI
  ): Either[CsvwLoadError, WithWarningsAndErrors[TableGroup]] = {
    try {
      fileUriToJson[ObjectNode](possibleSchemaUri)
        .flatMap(objectNode =>
          Schema.fromCsvwMetadata(possibleSchemaUri.toString, objectNode) match {
            case Right(tableGroup) => Right(tableGroup)
            case Left(metadataError) => Left(GeneralCsvwLoadError(metadataError))
          }
        )
        .flatMap(parsedTableGroup =>
            maybeCsvUri
              .map(csvUri => {
                val workingWithUserSpecifiedMetadata = schemaUri.isDefined && possibleSchemaUri.toString == schemaUri.get
                if (tableGroupContainsCsv(parsedTableGroup.component, csvUri) || parsedTableGroup.warningsAndErrors.errors.nonEmpty || workingWithUserSpecifiedMetadata) {
                  Right(parsedTableGroup)
                } else{
                  Left(
                    SchemaDoesNotContainCsvError(
                      new IllegalArgumentException(
                        s"Schema file does not contain a definition for $maybeCsvUri"
                      )
                    )
                  )
                }
              })
              .getOrElse(Right(parsedTableGroup))
        )
    } catch {
      case e: Throwable =>
        logger.debug(e)
        Left(GeneralCsvwLoadError(e))
    }
  }

  private def fileUriToJson[TJsonNode <: JsonNode](
      fileUri: URI
  ): Either[CsvwLoadError, TJsonNode] = {
    if (fileUri.getScheme == "file") {
      try {
        Right(objectMapper.readTree(new File(fileUri)).asInstanceOf[TJsonNode])
      } catch {
        case e: IOException => Left(CascadeToOtherFilesError(e))
      }
    } else {
      val response = httpClient.send(basicRequest.get(Uri(fileUri)))
      response.body match {
        case Left(error) => Left(GeneralCsvwLoadError(new Exception(error)))
        case Right(body) =>
          try {
            Right(objectMapper.readTree(body).asInstanceOf[TJsonNode])
          } catch {
            case e: JsonMappingException => Left(GeneralCsvwLoadError(e))
          }
      }
    }
  }

  private def tableGroupContainsCsv(
      tableGroup: TableGroup,
      csvUri: URI
  ): Boolean = {
    val csvUrl = csvUri.toString
    val tables = tableGroup.tables

    val csvUrlWithoutQueryString = getUriWithoutQueryString(csvUri).toString

    // todo: We need to be able to try both relative & absolute CSVURIs here.

    tables.contains(csvUrl) || tables.contains(csvUrlWithoutQueryString)
  }

  private def getUriWithoutQueryString(csvUri: URI): URI = {
    if (csvUri.getRawQuery == null)
      csvUri
    else {
      val queryStringLength = csvUri.getRawQuery.length
      val url = csvUri.toString
      new URI(url.substring(0, url.length - (queryStringLength + 1)))
    }
  }

  private def findAndValidateCsvwSchemaFileForCsv(
      maybeCsvUri: Option[URI],
      schemaUrisToCheck: Seq[URI]
  ): Source[WarningsAndErrors, NotUsed] = {
    schemaUrisToCheck match {
      case Seq() =>
        if (schemaUri.isDefined) {
          val error = ErrorWithCsvContext(
            "metadata",
            "cannot locate schema",
            "",
            "",
            s"${schemaUri.get} not found",
            ""
          )
          val warningsAndErrorsToReturn = List(
            models.WarningsAndErrors(errors = Array[ErrorWithCsvContext](error))
          )
          Source(warningsAndErrorsToReturn)
        } else Source(List(WarningsAndErrors()))
      case Seq(uri, uris @ _*) =>
        attemptToFindMatchingTableGroup(
          maybeCsvUri,
          uri
        ) match {
          case Right(parsedTableGroup) =>
            validateSchemaTables(parsedTableGroup.component).map { wAndE2 =>
              WarningsAndErrors(
                wAndE2.warnings ++ parsedTableGroup.warningsAndErrors.warnings,
                wAndE2.errors ++ parsedTableGroup.warningsAndErrors.errors
              )
            }

          case Left(GeneralCsvwLoadError(err)) =>
            val error = ErrorWithCsvContext(
              "metadata",
              err.getClass.getName,
              "",
              "",
              err.getMessage,
              ""
            )
            logger.debug(err)
            Source(List(WarningsAndErrors(errors = Array(error))))
          case Left(SchemaDoesNotContainCsvError(err)) =>
            logger.debug(err)
            findAndValidateCsvwSchemaFileForCsv(maybeCsvUri, uris).map(x => {
              WarningsAndErrors(
                x.warnings :+ WarningWithCsvContext(
                  "source_url_mismatch",
                  s"CSV supplied not found in metadata $uri",
                  "",
                  "",
                  "",
                  ""
                )
              )
            })
          case Left(CascadeToOtherFilesError(err)) =>
            logger.debug(err)
            val errorsAndWarnings =
              findAndValidateCsvwSchemaFileForCsv(maybeCsvUri, uris)
            errorsAndWarnings
          case Left(err) =>
            throw new IllegalArgumentException(s"Unhandled CsvwLoadError $err")

        }
    }
  }

  implicit val ec: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.global

  private def readAndValidateWithParser(
      schema: TableGroup,
      tableUri: URI,
      dialect: Dialect,
      format: CSVFormat,
      parser: CSVParser,
      degreeOfParallelismInTable: Int
  ): Source[
    (
        WarningsAndErrors,
        mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]],
        mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[
          KeyWithContext
        ]]
    ),
    NotUsed
  ] = {

    val table =
      getTable(schema, tableUri)

    val rowGrouping: Int = sys.env.get("ROW_GROUPING") match {
      case Some(value) => value.toInt
      case None        => 1000
    }

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
      .mapAsyncUnordered(degreeOfParallelismInTable)(csvRows =>
        Future {
          csvRows.map(parseRow(schema, tableUri, dialect, table, _))
        }
      )
      .fold[TableStateWithPrimaryKeyHashes](
        ArrayBuffer.empty[WarningWithCsvContext],
        ArrayBuffer.empty[ErrorWithCsvContext],
        mutable.Map(),
        mutable.Map(),
        mutable.Map()
      ) {

        /** The last item in AccumulatedValues is used to store the hashes of every primary keys and its row numbers.
          * To validate primary key efficiently, we need to have them in memory and HashSets gave the best performance.
          * Storing all of the primary keys in sets leads to huge memory usage by the application.
          * To be memory efficient, we hash the primary keys and then store them along with the rowNumbers.
          * By hashing primary key values we're only identifying possible duplicates and it can be an overestimate of
          * the actual number of duplicates because of hash collisions. Hash Collisions are also addressed at later stage
          *
          * {
          *  56234234234: {1, 20},
          *  45233453453: {2},
          *  234234234234: {345}
          * }
          */

        case (
              (
                warnings,
                errors,
                childTableForeignKeys,
                parentTableForeignKeys,
                mapPrimaryKeyHashToRowNumbers
              ),
              rowOutputs: Seq[ValidateRowOutput]
            ) =>
          extractInformationFromValidateRowOutputs(
            warnings,
            errors,
            childTableForeignKeys,
            parentTableForeignKeys,
            mapPrimaryKeyHashToRowNumbers,
            rowOutputs
          )

          (
            warnings,
            errors,
            childTableForeignKeys,
            parentTableForeignKeys,
            mapPrimaryKeyHashToRowNumbers
          )
      }
      .flatMapConcat(keyCheckAggregateValues => {
        // function getParser will return a parser here. If not, if wouldn't have reached here
        val newParser = getParser(tableUri, dialect, format)
          .getOrElse(
            throw new IllegalArgumentException("Could not fetch CSV parser")
          )
        checkPossiblePrimaryKeyDuplicates(
          keyCheckAggregateValues,
          newParser,
          rowGrouping,
          parallelism,
          table
        )
      })
  }

  private def extractInformationFromValidateRowOutputs(
      warnings: ArrayBuffer[WarningWithCsvContext],
      errors: ArrayBuffer[ErrorWithCsvContext],
      childTableForeignKeys: mutable.Map[ForeignKeyDefinition, mutable.Set[
        KeyWithContext
      ]],
      parentTableForeignKeys: mutable.Map[
        ReferencedTableForeignKeyReference,
        mutable.Set[KeyWithContext]
      ],
      mapPrimaryKeyHashToRowNumbers: mutable.Map[Int, ArrayBuffer[Long]],
      rowOutputs: Seq[ValidateRowOutput]
  ): Unit = {
    for (rowOutput <- rowOutputs) {
      errors.addAll(rowOutput.warningsAndErrors.errors)
      warnings.addAll(rowOutput.warningsAndErrors.warnings)
      setChildTableForeignKeys(
        rowOutput,
        childTableForeignKeys
      )
      setParentTableForeignKeyReferences(
        rowOutput,
        parentTableForeignKeys
      )
      accumulatePossiblePrimaryKeyDuplicates(
        mapPrimaryKeyHashToRowNumbers,
        rowOutput
      )
    }
  }

  /**
    * Since the primary key validation was done based on the hashes of primaryKeys in each row, there could be
    * primaryKey errors reported because of collisions.
    * For example primary key values of 2 different rows can have the same hash even when they are NOT the same.
    * This means that we could have false negatives in primaryKey errors.
    * To fix this, all the hashes which contain more than one rowNumber is checked again and the primary key error is set
    * at this point. During this checking the actual values of primary keys of these rows are compared.
    */
  private def checkPossiblePrimaryKeyDuplicates(
      keyCheckAggregateValues: TableStateWithPrimaryKeyHashes,
      parser: CSVParser,
      rowGrouping: Int,
      parallelism: Int,
      table: Table
  ): Source[
    (
        WarningsAndErrors,
        mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]],
        mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[
          KeyWithContext
        ]]
    ),
    NotUsed
  ] = {
    val (
      warnings,
      errors,
      childTableForeignKeys,
      parentTableForeignKeys,
      mapPrimaryKeyHashToRowNumbers
    ) = keyCheckAggregateValues
    val rowsToCheckAgain = mapPrimaryKeyHashToRowNumbers
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
          csvRows.map(table.validateRow)
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
              .ifDefined(e => {
                errorsInAkkaStreams.addOne(e)
              })
          }
          (primaryKeyValues, errorsInAkkaStreams)
      }
      .map {
        case (_, err) =>
          (
            models.WarningsAndErrors(
              warnings.toArray[WarningWithCsvContext],
              ArrayBuffer.concat(errors, err).toArray
            ),
            childTableForeignKeys,
            parentTableForeignKeys
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
          s"key already present - ${getListStringValue(primaryKeyValues)}",
          ""
        )
      )
    } else {
      existingPrimaryKeyValues += primaryKeyValues
      None
    }
  }

  private def parseRow(
      tableGroup: TableGroup,
      tableUri: URI,
      dialect: Dialect,
      table: Table,
      row: CSVRecord
  ): ValidateRowOutput = {
    if (row.getRecordNumber == 1 && dialect.header) {
      val warningsAndErrors = tableGroup.validateHeader(row, tableUri.toString)
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
        table.schema
          .map(s => {
            if (s.columns.length >= row.size()) {
              table.validateRow(row)
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
      mapPrimaryKeyHashToRowNumbers: mutable.Map[Int, ArrayBuffer[Long]],
      validateRowOutput: ValidateRowOutput
  ): Unit = {
    val primaryKeyValueHash = validateRowOutput.primaryKeyValues.hashCode()
    if (validateRowOutput.primaryKeyValues.nonEmpty) {
      mapPrimaryKeyHashToRowNumbers.get(primaryKeyValueHash) match {
        case Some(_) =>
          mapPrimaryKeyHashToRowNumbers(primaryKeyValueHash).addOne(
            validateRowOutput.recordNumber
          )
        case None =>
          mapPrimaryKeyHashToRowNumbers.addOne(
            primaryKeyValueHash -> ArrayBuffer(validateRowOutput.recordNumber)
          )
      }
    }
  }

  private def setParentTableForeignKeyReferences(
      validateRowOutput: ValidateRowOutput,
      parentTableForeignKeyReferences: mutable.Map[
        ReferencedTableForeignKeyReference,
        mutable.Set[
          KeyWithContext
        ]
      ]
  ): Unit = {
    validateRowOutput.parentTableForeignKeyReferences
      .foreach {
        case (k, value) =>
          val allPossibleParentKeyValues =
            parentTableForeignKeyReferences.getOrElse(
              k,
              mutable.Set[KeyWithContext]()
            )
          if (allPossibleParentKeyValues.contains(value)) {
            allPossibleParentKeyValues -= value
            value.isDuplicate = true
          }
          allPossibleParentKeyValues += value
          parentTableForeignKeyReferences(k) = allPossibleParentKeyValues
      }
  }

  private def setChildTableForeignKeys(
      validateRowOutput: ValidateRowOutput,
      childTableForeignKeys: mutable.Map[ForeignKeyDefinition, mutable.Set[
        KeyWithContext
      ]]
  ): Unit = {
    validateRowOutput.childTableForeignKeys
      .foreach {
        case (k, value) =>
          val childKeyValues = childTableForeignKeys.getOrElse(
            k,
            mutable.Set[KeyWithContext]()
          )
          childKeyValues += value
          childTableForeignKeys(k) = childKeyValues
      }
  }

  private def validateForeignKeyReferences(
      childTableForeignKeysByTable: mutable.Map[
        Table,
        mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]]
      ],
      parentTableForeignKeyReferencesByTable: mutable.Map[
        Table,
        mutable.Map[ReferencedTableForeignKeyReference, mutable.Set[KeyWithContext]]
      ]
  ): Errors = {
    // Child Table : Parent Table
    // Country, Year, Population  : Country, Name
    // UK, 2021, 67M  : UK, United Kingdom
    // EU, 2021, 448M : EU, Europe
    var errors: Errors = Array[ErrorWithCsvContext]()
    for (
      (parentTable, mapParentTableForeignKeyReferenceToAllPossibleValues) <-
        parentTableForeignKeyReferencesByTable
    ) {
      for (
        (parentTableForeignKeyReference, allPossibleParentTableValues) <-
          mapParentTableForeignKeyReferenceToAllPossibleValues
      ) {
        val childTableForeignKeys
            : mutable.Map[ForeignKeyDefinition, mutable.Set[KeyWithContext]] =
          childTableForeignKeysByTable
            .getOrElse(
              parentTableForeignKeyReference.definitionTable,
              throw new Exception(
                s"Could not find corresponding child table(${parentTableForeignKeyReference.definitionTable.url}) for parent table ${parentTable.url}"
              )
            )

        val childTableKeyValues: mutable.Set[KeyWithContext] =
          childTableForeignKeys
            .getOrElse(
              parentTableForeignKeyReference.foreignKey,
              throw new Exception(
                s"Could not find foreign key against child table." + parentTableForeignKeyReference.foreignKey.jsonObject.toPrettyString
              )
            )

        val keyValuesNotDefinedInParent =
          childTableKeyValues.diff(allPossibleParentTableValues)
        if (keyValuesNotDefinedInParent.nonEmpty) {
          errors ++= keyValuesNotDefinedInParent
            .map(k =>
              ErrorWithCsvContext(
                "unmatched_foreign_key_reference",
                "schema",
                k.rowNumber.toString,
                "",
                getListStringValue(k.keyValues),
                ""
              )
            )
        }

        val duplicateKeysInParent = allPossibleParentTableValues
          .intersect(childTableKeyValues)
          .filter(k => k.isDuplicate)

        if (duplicateKeysInParent.nonEmpty) {
          errors ++= duplicateKeysInParent
            .map(k =>
              ErrorWithCsvContext(
                "multiple_matched_rows",
                "schema",
                k.rowNumber.toString,
                "",
                getListStringValue(k.keyValues),
                ""
              )
            )
        }
      }
    }
    errors
  }

  private def getTable(schema: TableGroup, tableUri: URI) = {
    try {
      schema.tables(tableUri.toString)
    } catch {
      case NonFatal(e) =>
        logger.debug(e)
        throw MetadataError(
          "Metadata does not contain requested tabular data file"
        )
    }
  }

  private def getListStringValue(list: List[Any]): String = {
    val stringList = list.map {
      case listOfAny: List[Any] =>
        listOfAny.map(s => s.toString).mkString(",")
      case i => i.toString
    }
    stringList.mkString(",")
  }

}
