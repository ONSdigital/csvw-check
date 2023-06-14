package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.{MetadataWarningsExtensions, assertJsonNodesEquivalent, assertObjectNormalisation, jsonToNode, starterContext}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.IteratorHasAsScala

class TransformationNormalisionTests extends AnyFunSuite {
    // Transformations Property tests
    test(
      "return the entire transformations without warnings when provided with valid transformations array"
    ) {
      val warnings = assertObjectNormalisation(
        Transformation.normalisers,
        """
          |{
          | "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
          | "titles": "Simple XML version",
          | "url": "xml-template.mustache",
          | "scriptFormat": "https://mustache.github.io/",
          | "source": "json"
          |}
          |""".stripMargin,
        """
          |{
          | "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
          | "titles": {
          |   "und": ["Simple XML version"]
          | },
          | "url": "https://example.com/xml-template.mustache",
          | "scriptFormat": "https://mustache.github.io/",
          | "source": "json"
          |}
          |""".stripMargin
      )

      assert(warnings.isEmpty)
    }

    test(
      "return transformations after stripping off invalid transformation objects"
    ) {
      val transformationsArray = jsonToNode[ArrayNode](
        """
          | [{
          |    "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
          |    "titles": "Simple XML version",
          |    "url": "xml-template.mustache",
          |    "scriptFormat": "https://mustache.github.io/",
          |    "source": "json",
          |    "textDirection": "Some value"
          |  }]
          |""".stripMargin)

      val Right((normalisedTransformationsArray, warnings, _)) = Transformation.normaliseTransformationsProperty(PropertyType.Undefined)(
        starterContext.withNode(transformationsArray)
      )

      assert(warnings.containsWarningWith("invalid_property"))

      val expectedTransformationsArray = jsonToNode[ArrayNode](
        """
          |[{
          | "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
          | "titles": {
          |   "und": ["Simple XML version"]
          | },
          | "url": "https://example.com/xml-template.mustache",
          | "scriptFormat": "https://mustache.github.io/",
          | "source": "json"
          |}]
          |""".stripMargin
      )

      assertJsonNodesEquivalent(normalisedTransformationsArray, expectedTransformationsArray)
    }

    test("throw exception when transformation objects cannot be processed") {
      val transformationsArray = jsonToNode[ArrayNode](
        """
          | [{
          |    "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
          |    "@id": "_:nooo",
          |    "titles": "Simple XML version"
          |  }]
          |""".stripMargin
      )

      val Left(metadataError) = Transformation.normaliseTransformationsProperty(PropertyType.Undefined)(
        starterContext.withNode(transformationsArray)
      )

      assert(metadataError.message == "'_:nooo' starts with _:")
    }

}
