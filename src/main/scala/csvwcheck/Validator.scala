package csvwcheck

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
import com.typesafe.scalalogging.Logger
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.errors._
import csvwcheck.models._
import csvwcheck.traits.LoggerExtensions.LogDebugException
import sttp.client3.{HttpClientSyncBackend, Identity, SttpBackend, basicRequest, ignore}
import sttp.model.Uri

import java.io.{File, IOException}
import java.net.{URI, URL}
import scala.language.postfixOps

class Validator(
                 val schemaUri: Option[String],
                 csvUrl: Option[String] = None,
                 httpClient: SttpBackend[Identity, Any] = HttpClientSyncBackend(),
                 numParallelThreads: Int = 1,
                 csvRowBatchSize: Int = 1000
               ) {
  private val logger = Logger(this.getClass.getName)
  private val csvwLinkHeaderRegEx = "^\\s*<(.*?)>\\s*;.*$".r

  def getSchemaLocationSuggestedByHttpHeaders(csvUri: URI): Option[URI] = {
    val uriScheme = csvUri.getScheme
    if (uriScheme == "http" || uriScheme == "https") {
      val csvHttpResponse =
        httpClient.send(basicRequest.response(ignore).get(Uri(csvUri)))
      csvHttpResponse
        .header("Link")
        .map(header => {
          val metadataJsonLocation =
            csvwLinkHeaderRegEx.replaceAllIn(header, "$1")
          // Now make the URL absolute if it isn't already.
          new URL(
            new URL(getUriWithoutQueryString(csvUri).toString),
            metadataJsonLocation
          ).toURI
        })
    } else {
      None
    }
  }

  def validate(): Source[WarningsAndErrors, NotUsed] = {
    val absoluteSchemaUri = schemaUri.map(getAbsoluteSchemaUri)

    val csvUri = csvUrl.map(new URI(_))

    val schemaUrisToCheck = csvUri
      .map(csvUri => {
        Array(
          getSchemaLocationSuggestedByHttpHeaders(csvUri),
          absoluteSchemaUri,
          Some(new URI(s"${getUriWithoutQueryString(csvUri)}-metadata.json")),
          Some(csvUri.resolve("csv-metadata.json"))
        )
      })
      .getOrElse(Array(absoluteSchemaUri))
      .flatten
      .distinct

    findAndValidateCsvwSchemaFileForCsv(csvUri, schemaUrisToCheck.toSeq)
  }

  private def getAbsoluteSchemaUri(schemaPath: String): URI = {
    val inputSchemaUri = new URI(schemaPath)
    if (inputSchemaUri.getScheme == null) {
      new URI(s"file://${new File(schemaPath).getAbsolutePath}")
    } else {
      inputSchemaUri
    }
  }

  private def tryParsingPossibleSchema(
                                        maybeCsvUri: Option[URI],
                                        possibleSchemaUri: URI
                                      ): Either[CsvwLoadError, WithWarningsAndErrors[TableGroup]] = {
    try {
      fileUriToJson[ObjectNode](possibleSchemaUri)
        .flatMap(tableGroupNode => normaliseTableGroup(possibleSchemaUri, tableGroupNode))
        .flatMap(normalisedTableGroup =>
          maybeCsvUri
            .map(csvUri => {
              val workingWithUserSpecifiedMetadata =
                schemaUri.isDefined && possibleSchemaUri.toString == schemaUri.get
              if (
                tableGroupContainsCsv(
                  normalisedTableGroup.component,
                  csvUri
                ) || normalisedTableGroup.warningsAndErrors.errors.nonEmpty || workingWithUserSpecifiedMetadata
              ) {
                Right(normalisedTableGroup)
              } else {
                Left(
                  SchemaDoesNotContainCsvError(
                    new IllegalArgumentException(
                      s"Schema file does not contain a definition for $maybeCsvUri"
                    )
                  )
                )
              }
            })
            .getOrElse(Right(normalisedTableGroup))
        )
    } catch {
      case e: Throwable =>
        logger.debug(e)
        Left(GeneralCsvwLoadError(e))
    }
  }

  private def normaliseTableGroup(possibleSchemaUri: URI, tableGroupNode: ObjectNode): Either[GeneralCsvwLoadError, WithWarningsAndErrors[TableGroup]] = {
    val normalisedTableGroupWithWarningsAndErrors = normalisation.TableGroup.normaliseTableGroup(tableGroupNode, possibleSchemaUri.toString)
      .flatMap({ case (normalisedTableGroup, warnings) =>
        for {
          tableGroupWithWarningsAndErrors <- TableGroup.fromJson(normalisedTableGroup)
        } yield {
          val newWarningsAndErrors = tableGroupWithWarningsAndErrors.warningsAndErrors
            .copy(warnings = tableGroupWithWarningsAndErrors.warningsAndErrors.warnings ++ warnings)
          tableGroupWithWarningsAndErrors.copy(warningsAndErrors = newWarningsAndErrors)
        }
      })

    // Convert this result to the correct local Either type.
    normalisedTableGroupWithWarningsAndErrors match {
      case Right(tableGroupWithWarningsAndErrors) => Right(tableGroupWithWarningsAndErrors)
      case Left(metadataError) =>
        Left(GeneralCsvwLoadError(metadataError))
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
        case Left(error) => Left(CascadeToOtherFilesError(new Exception(error)))
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
        schemaUri
          .map(_ => {
            val error = ErrorWithCsvContext(
              "metadata",
              "cannot locate schema",
              "",
              "",
              s"${schemaUri.get} not found",
              ""
            )
            Source(
              List(
                models
                  .WarningsAndErrors(errors = Array[ErrorWithCsvContext](error))
              )
            )
          })
          .getOrElse(Source(List(WarningsAndErrors())))
      case Seq(uri, uris@_*) => tryNextPossibleSchema(maybeCsvUri, uri, uris)
    }
  }

  private def tryNextPossibleSchema(
                                     maybeCsvUri: Option[URI],
                                     nextSchemaUri: URI,
                                     untestedSchemaUris: Seq[URI]
                                   ): Source[WarningsAndErrors, NotUsed] = {
    tryParsingPossibleSchema(
      maybeCsvUri,
      nextSchemaUri
    ) match {
      case Right(normalisedTableGroupWithWarningsAndErrors) =>
        val normalisedTableGroup = normalisedTableGroupWithWarningsAndErrors.component
        normalisedTableGroup
          .validateCsvsAgainstTables(numParallelThreads, csvRowBatchSize)
          .map { wAndE2 =>
            WarningsAndErrors(
              wAndE2.warnings ++ normalisedTableGroupWithWarningsAndErrors.warningsAndErrors.warnings,
              wAndE2.errors ++ normalisedTableGroupWithWarningsAndErrors.warningsAndErrors.errors
            )
          }
      case Left(GeneralCsvwLoadError(err)) =>
        val errorWithCsvContext = err match {
          case metadataError: MetadataError =>
            val cause = if (metadataError.cause == null) {
              ""
            } else {
              s"(Caused by ${metadataError.cause.getMessage})"
            }

            ErrorWithCsvContext(
              "metadata",
              metadataError.getClass.getName,
              "",
              "",
              s"${metadataError.propertyPath.mkString(".")}: ${metadataError.message} $cause",
              ""
            )
          case generalError => ErrorWithCsvContext(
            "unknown",
            generalError.getClass.getName,
            "",
            "",
            generalError.getMessage,
            ""
          )
        }

        logger.debug(err)
        Source(List(WarningsAndErrors(errors = Array(errorWithCsvContext))))
      case Left(SchemaDoesNotContainCsvError(err)) =>
        logger.debug(err)
        findAndValidateCsvwSchemaFileForCsv(maybeCsvUri, untestedSchemaUris)
          .map(warningsAndErrors =>
            warningsAndErrors.copy(warnings =
              warningsAndErrors.warnings :+ WarningWithCsvContext(
                "source_url_mismatch",
                s"CSV supplied not found in metadata $nextSchemaUri",
                "",
                "",
                "",
                ""
              )
            )
          )
      case Left(CascadeToOtherFilesError(err)) =>
        logger.debug(err)
        findAndValidateCsvwSchemaFileForCsv(maybeCsvUri, untestedSchemaUris)
      case Left(err) =>
        throw new IllegalArgumentException(s"Unhandled CsvwLoadError $err")
    }
  }

  implicit val ec: scala.concurrent.ExecutionContext =
    scala.concurrent.ExecutionContext.global
}
