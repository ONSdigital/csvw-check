package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.TextNode
import csvwcheck.TestPaths.resourcesPath
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.{assertJsonNodesEquivalent, jsonToObjectNode, starterContext}
import org.scalatest.funsuite.AnyFunSuite


class DialectNormalisationTests extends AnyFunSuite {
  test("support loading dialect from a separate document") {
    val testCaseFileUri = resourcesPath
      ./("csvwExamples")
      ./("dialectResource.json")
      .path
      .toAbsolutePath
      .toUri

    val Right((normalisedValue, warnings, _)) = Dialect.normaliseDialectProperty(PropertyType.Undefined)(
        starterContext.withNode(new TextNode(testCaseFileUri.toString))
      )

    assert(warnings.isEmpty)

    val expectedNormalisedNode = jsonToObjectNode(
      """
        | {
        |   "delimiter": "|"
        | }
        |""".stripMargin)

    assertJsonNodesEquivalent(normalisedValue, expectedNormalisedNode)
  }

}
