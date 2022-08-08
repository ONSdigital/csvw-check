package csvwcheck.steps

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import csvwcheck.Validator
import csvwcheck.models.WarningsAndErrors
import io.cucumber.scala.{EN, ScalaDsl}
import org.slf4j.LoggerFactory
import sttp.client3._
import sttp.client3.testing._
import sttp.model._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.net.URI
import java.net.MalformedURLException

import csvwcheck.models.Schema
import csvwcheck.models.TableGroup

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
  * Where it is scenario (A) we read the test content in immediately.
  * Where it is (B) a second step is required to specify the fixture holding
  * the test content for said remote resource (see "...equates to..." steps).
  */
  def setUri(fileOrUrl: String) {
      try {
        uri = new URI(fileOrUrl)
      } catch {
        case e: MalformedURLException =>
          var fixtureUri: String = fixturesPath + fileOrUrl
          uri = new URI(fixtureUri)
        case e: Throwable => println(s"Unable to set URI for $fileOrUrl")
      }

      // If the uri is pointing to a local file, read it in now
      if (uri.getScheme == "file") {
        setContent()
      }
  }

  def setHeaders(headerString: String) {
    if (uri.getScheme == "file") {
        throw new RuntimeException(s"Will not set mock response headers for local file $uri, that doesn't make sense.")
      }
 
    // TODO = string to Seq[Header]
  }

  // Parse the provided fixture file
  def setContent(fileName: Option[String] = None) {
    assert(content == "", s"Test content for resource $uri has already been set.")
    if (uri.getScheme == "file") {
        throw new RuntimeException(s"URI $uri is already a local file, you don't need to specify a fixture to represent it.")
      }
    // TODO
    // 1.) IF fileName, create a fixture path as URI() with fixturesPath + fileName
    // 2.) Get the correct parser to use (csv or json) from the file extension
    // 3.) Read the content in.
  }
}

class StepDefinitions extends ScalaDsl with EN {
  private var warningsAndErrors: WarningsAndErrors = WarningsAndErrors()
  private var schemaResource: CommonTestFileResource = new CommonTestFileResource()
  private var csvResource: CommonTestFileResource = new CommonTestFileResource()


  // Assume we use sttp as http client.
  // Call this funciton and set the testing backend object. Pass the backend object into validator function
  // Stubbing for sttp is done as given in their docs at https://sttp.softwaremill.com/en/latest/testing.html
  def setTestingBackend(): Unit = {
    SttpBackendStub.synchronous
      .whenRequestMatchesPartial({
        case r if r.uri.path.startsWith(List(csvResource.uri)) =>
          Response.apply(csvResource.content, StatusCode.Ok, "OK", csvResource.headers)
        case r if r.uri.path.startsWith(List(schemaResource.uri)) =>
          Response.ok(schemaResource.content)
        case _ =>
          Response("Not found", StatusCode.NotFound)
      })
  }

  Given("""^I have a csv file "(.*?)"$""") { (fileOrUrl: String) =>
    csvResource.setUri(fileOrUrl)
  }

  Given("""^I have a csv file "([^"]*)"" with the headers "([^"]*)"$""") { (fileOrUrl: String, headerString: String) =>
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
    
    // Ensure the provided resource uris have test content associated with them. 
    assert(schemaResource.content != "")
    assert(csvResource.content != "")

    val validator = new Validator(
      Some(schemaResource.uri.toString()),
      Some(csvResource.uri.toString())
      )
    val akkaStream =
      validator.validate().map(wAndE => warningsAndErrors = wAndE)
    Await.ready(akkaStream.runWith(Sink.ignore), Duration.Inf)
  }

  Then("there should not be errors") { () =>
    assert(warningsAndErrors.errors.length == 0, warningsAndErrors.warnings.map(w => w.toString).mkString(", "))
    }

  And("there should not be warnings") { () =>
      assert(warningsAndErrors.warnings.length == 0, warningsAndErrors.warnings.map(w => w.toString).mkString(", "))
  }

  Then("there should be errors") { () =>
    assert(warningsAndErrors.errors.length > 0, "Errors expected but 0 errors encountered")
  }

  Then("""there should be warnings""") { () =>
    assert(warningsAndErrors.warnings.length > 0, "Warnings expected but 0 warnings encountered")
  }
}
