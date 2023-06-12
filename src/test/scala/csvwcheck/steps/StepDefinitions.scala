package csvwcheck.steps

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import better.files.Dsl.unzip
import better.files.File
import csvwcheck.Validator
import csvwcheck.models.WarningsAndErrors
import io.cucumber.scala.{EN, ScalaDsl}
import sttp.client3._
import sttp.client3.testing._
import sttp.model._

import java.net.URI
import java.nio.file.Files
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.Source

class StepDefinitions extends ScalaDsl with EN {
  val fixturesPath: File = File(System.getProperty("user.dir"))
    ./("src")
    ./("test")
    ./("resources")
    ./("features")
    ./("fixtures")
  private var warningsAndErrors: WarningsAndErrors = WarningsAndErrors()
  private var schemaUrl: Option[String] = None
  private var csvUrl: Option[String] = None
  private var httpMock: SttpBackendStub[Identity, _] =
    SttpBackendStub.synchronous
  private var currentFileMock: Option[RemoteHttpFileMock] = None

  def ensureFixtureFilesDownloaded(): Unit = {
    val expectedFixtureFile = fixturesPath./("test001.csv")
    if (!expectedFixtureFile.exists) {
      fixturesPath.synchronized {
        if (!expectedFixtureFile.exists) {
          val tmpDir = File(
            Files.createTempDirectory("csvw-check-test-fixtures").toFile.toPath
          )
          tmpDir.deleteOnExit()

          val zipLocalLocation = tmpDir./("data.zip")
          // Download the zip file containing the fixtures into the temp dir
          zipLocalLocation
            .fileOutputStream()
            .map(outputStream =>
              HttpClientSyncBackend()
                .send(
                  basicRequest
                    .response(asByteArray)
                    .get(
                      Uri(
                        new URI(
                          "https://github.com/w3c/csvw/archive/refs/heads/gh-pages.zip"
                        )
                      )
                    )
                )
                .body
                .map(outputStream.write)
                .getOrElse(new Exception("Failed to download the test cases"))
            )

          unzip(zipLocalLocation)(tmpDir)
          tmpDir
            ./("csvw-gh-pages")
            ./("tests")
            .children
            .foreach(_.copyToDirectory(fixturesPath))
        }
      }
    }

    assert(expectedFixtureFile.exists, s"$expectedFixtureFile doesn't exist")
  }

  private def mockCurrentFileResponse(): Unit =
    currentFileMock.foreach(fileMock => {
      httpMock = httpMock
        .whenRequestMatches(req => {
          req.uri.toString == fileMock.remoteFileUrl
        })
        .thenRespondF({ req =>
          fileMock.localFileName
            .map(localFileName => {
              val localFilePath = fixturesPath./(localFileName)
              val source = Source.fromFile(localFilePath.toString, "utf-8")
              val fileResponseBody = if(req.response.toString.contains("File")) {
                // Response wants a file.
                Right(new java.io.File(localFilePath.toString))
              } else {
                source.getLines().mkString
              }

              fileMock.linkHeader
                .map(linkHeaderValue =>
                  Response(
                    fileResponseBody,
                    StatusCode.Ok,
                    "OK",
                    Seq(Header("Link", linkHeaderValue))
                  )
                )
                .getOrElse(Response(fileResponseBody, StatusCode.Ok))
            })
            .getOrElse(Response("", StatusCode.NotFound))
        })
      currentFileMock = None
    })

  private case class RemoteHttpFileMock(
                                         remoteFileUrl: String,
                                         localFileName: Option[String],
                                         linkHeader: Option[String]
                                       )

  Given("^The test-cases defined by the W3C$") { () => ensureFixtureFilesDownloaded() }

  Given("""^I have a CSV file called "(.*?)"$""") { (filename: String) =>
    mockCurrentFileResponse()
    currentFileMock = Some(
      RemoteHttpFileMock(
        remoteFileUrl = "UNSET",
        localFileName = Some(filename),
        linkHeader = None
      )
    )
  }

  Given("""^it is stored at the url "(.*?)"$""") { (url: String) =>
    csvUrl = Some(url)
    currentFileMock = currentFileMock.map(_.copy(remoteFileUrl = url))
  }

  Given("""^there is no file at the url "(.*?)"$""") { (url: String) =>
    mockCurrentFileResponse()
    currentFileMock = Some(
      RemoteHttpFileMock(
        remoteFileUrl = url,
        localFileName = None,
        linkHeader = None
      )
    )
  }

  Given("""^I have a metadata file called "([^"]*)"$""") { fileName: String =>
    mockCurrentFileResponse()
    currentFileMock = Some(
      RemoteHttpFileMock(
        remoteFileUrl = "UNSET",
        localFileName = Some(fileName),
        linkHeader = None
      )
    )
  }

  And("""^the (schema|metadata) is stored at the url "(.*?)"$""") {
    (_: String, url: String) => {
      schemaUrl = Some(url)
      currentFileMock = currentFileMock.map(_.copy(remoteFileUrl = url))
    }
  }

  Given("""^it has a Link header holding "(.*?)"$""") { l: String =>
    currentFileMock = currentFileMock.map(_.copy(linkHeader = Some(l)))
  }

  And("""^I have a file called "(.*?)" at the url "(.*?)"$""") {
    (localFile: String, url: String) =>
      mockCurrentFileResponse()
      currentFileMock = Some(
        RemoteHttpFileMock(
          remoteFileUrl = url,
          localFileName = Some(localFile),
          linkHeader = None
        )
      )
  }

  When("I carry out CSVW validation") { () =>
    mockCurrentFileResponse()

    implicit val system: ActorSystem = ActorSystem("actor-system")
    val validator = new Validator(schemaUrl, csvUrl, httpClient = httpMock)
    val akkaStream =
      validator.validate().map(wAndE => warningsAndErrors = wAndE)
    Await.ready(akkaStream.runWith(Sink.ignore), Duration.Inf)
  }

  Then("there should not be errors") { () =>
    assert(
      warningsAndErrors.errors.length == 0,
      warningsAndErrors.errors.map(_.toString).mkString(", ")
    )
  }

  And("there should not be warnings") { () =>
    assert(
      warningsAndErrors.warnings.length == 0,
      warningsAndErrors.warnings.map(_.toString).mkString(", ")
    )
  }

  Then("there should be errors") { () =>
    assert(warningsAndErrors.errors.length > 0)
  }

  Then("""there should be warnings""") { () =>
    assert(warningsAndErrors.warnings.length > 0)
  }
}
