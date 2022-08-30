package csvwcheck.steps

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import csvwcheck.Validator
import csvwcheck.models.WarningsAndErrors
import io.cucumber.scala.{EN, ScalaDsl}
import org.slf4j.LoggerFactory
import sttp.client3._
import sttp.client3.testing._
import sttp.model.Header
import sttp.model._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.net.URI

// Common superclass for SchemaResource and CsvResource to hold shared methods.
class CommonTestFileResource {
  private val fixturesPath = "src/test/resources/features/fixtures/"
  var uri: URI = _
  var content: String = _
  var headers: Seq[Header] = _

  /*
  Every resource has a URI, this can be:
  * (a) A local resource starting file://
  * (b) A remote resource starting http:// or https://
  * Where it is (B) a second step will be required to specify the fixture holding
  * the mock http content for the testingBackend.
  * See the "...equates to..." steps).
  */
  def setUri(fileOrUrl: String): Unit = {
      uri = new URI(fileOrUrl)

      // If the URI does not have a scheme we're assuming it's a local file
      // on a path relative to the fixtures dir.
      if (uri.getScheme != "file" && uri.getScheme != "http" && uri.getScheme != "https") {
        var fixtureUri: String = fixturesPath + fileOrUrl
        uri = new URI(fixtureUri)
      }
  }

  def setHeaders(headerString: String): Unit = {
    if (uri.getScheme == "file") {
      throw new RuntimeException(s"Will not set mock response headers for local file $uri, that doesn't make sense.")
      }

    // TODO = convert contents of headerString to Seq[Header] and populate .header
  }

  // Where the uri is a remote resource, we setContent to use a local fixture to
  // mock the http response body
  def setContent(fileName: String): Unit = {
    assert(content == null, s"Test content for resource $uri has already been set")

    if (uri.getScheme == "file") {
        throw new RuntimeException(s"URI $uri is already a local file, you don't need to specify a fixture to represent it.")
      }

    val fixtureLocalUri: URI = URI.create(fixturesPath + fileName)
    val extension: String = fileName.substring(fileName.lastIndexOf("."))

    // TODO - we need to somehow set the local content to how it would be represented
    // as a http response body.
    extension match {
      case "csv" => {
        // Mock csv response body with content from fixtureLocalUri
      }
      case "json" => {
        // Mock json response body with content from fixtureLocalUri
      }
      case _ => {
        // User input error
        throw new RuntimeException(s"Specified testing resource $fileName does not appear to be a .csv or .json file.")
      }
    }
  }
}

class StepDefinitions extends ScalaDsl with EN {
  private var warningsAndErrors: WarningsAndErrors = WarningsAndErrors()
  private var schemaResource: CommonTestFileResource = new CommonTestFileResource()
  private var csvResource: CommonTestFileResource = new CommonTestFileResource()

  Given("""^I have a csv file "([^\s]+\.csv)"$""") { (fileOrUrl: String) =>
    csvResource.setUri(fileOrUrl)
  }

  Given("""^I have a csv file "([^\s^"]+\.csv)" with the headers \"{3}(.*?)\"{3}$""") { (fileOrUrl: String, headerString: String) =>
    csvResource.setUri(fileOrUrl)
    csvResource.setHeaders(headerString)
  }

  Given("""^the csv file equates to the test fixture "([^"]*)"$""") { (featureFileName: String) =>
    csvResource.setContent(featureFileName)
  }

  Given("""^I have a metadata file "([^"]*)"$""") { (fileOrUrl: String) =>
    schemaResource.setUri(fileOrUrl)
  }

  Given("""^the metadata file equates to the test fixture "([^"]*)"$""") { (featureFileName: String) =>
    schemaResource.setContent(featureFileName)
  }

  When("I carry out CSVW validation") { () =>
    implicit val system: ActorSystem = ActorSystem("actor-system")

    // Make sure the minimum required uris have been explicitly set.
    assert(schemaResource.uri != "", "A schema must be provided")
    assert(csvResource.uri != "", "A csv must be provided")

    val validator = new Validator(
      Some(schemaResource.uri.toString()),
      Some(csvResource.uri.toString()),

      // TODO - "sttpBacked" provides our stubbed/mock http responses for testing.
      // In some case there will be (as I understand it) some resources that are
      // dependencies of other resources, this will need some figuring out and
      // updating when we get to those scenarios. "A" (note singular) response mock
      // per file type is likely be insufficient in these cases.
      // It may be that you need a list/array of CommonTestFileResource(s) and appropriate
      // steps to populate - unsure at this time.
      sttpBackend = SttpBackendStub.synchronous
      .whenRequestMatchesPartial({
        case r if (r.uri.toString() == Uri(csvResource.uri).toString()) =>
          Response.apply(csvResource.content, StatusCode.Ok, "OK", csvResource.headers)
        case r if (r.uri.toString() == Uri(schemaResource.uri).toString()) =>
          Response.ok(schemaResource.content)
        case r =>
          val uriString: String = r.uri.path.toString()
          Response(s"Backend Stub - Url $uriString Not found", StatusCode.NotFound)
      })
    
      )
    val akkaStream =
      validator.validate().map(wAndE => warningsAndErrors = wAndE)
    Await.ready(akkaStream.runWith(Sink.ignore), Duration.Inf)
  }

  Then("there should not be errors") { () =>
    var errorString = warningsAndErrors.errors.map(w => w.toString).mkString(", ")
    assert(warningsAndErrors.errors.length == 0, s"Errors were $errorString")
    }

  And("there should not be warnings") { () =>
    var warningString = warningsAndErrors.warnings.map(w => w.toString).mkString(", ")
    assert(warningsAndErrors.errors.length == 0, s"Warnings were $warningString")
    }

  Then("there should be errors") { () =>
    assert(warningsAndErrors.errors.length > 0, "Errors expected but 0 errors encountered")
  }

  Then("""there should be warnings""") { () =>
    assert(warningsAndErrors.warnings.length > 0, "Warnings expected but 0 warnings encountered")
  }

}
