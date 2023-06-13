package csvwcheck.normalisation

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{NullNode, ObjectNode}
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.errors.MetadataError
import csvwcheck.normalisation.Utils.{MetadataWarnings, NormalisationContext, Normaliser}
import sttp.client3.HttpClientSyncBackend

object NormalisationTestUtils {
  private val httpClient = HttpClientSyncBackend()
  val starterContext = NormalisationContext(
    node = NullNode.instance,
    baseUrl = "https://example.com/",
    language = "und",
    propertyPath = Array(),
    httpClient = httpClient
  )

  def assertObjectNormalisation(normalisers: Map[String, Normaliser], unNormalisedObjectNodeJson: String, expectedNormalisedObjectNodeJson: String): MetadataWarnings = {
    val Right((normalisedObject, warnings)) = Utils.normaliseObjectNode(
      normalisers,
      starterContext.withNode(objectMapper.readTree(unNormalisedObjectNodeJson).asInstanceOf[ObjectNode])
    )
    val expectedNormalisedObjectNode = objectMapper.readTree(expectedNormalisedObjectNodeJson).asInstanceOf[ObjectNode]
    assertJsonNodesEquivalent(normalisedObject, expectedNormalisedObjectNode)

    warnings
  }

  def assertJsonNodesEquivalent(actual: JsonNode, expected: JsonNode) = {
    assert(actual == expected, s"${actual.toPrettyString} != ${expected.toPrettyString}")
  }

  def assertObjectNormalisationFailure(normalisers: Map[String, Normaliser], unNormalisedObjectNodeJson: String): MetadataError = {
    val Left(metadataError) = Utils.normaliseObjectNode(
      normalisers,
      starterContext.withNode(objectMapper.readTree(unNormalisedObjectNodeJson).asInstanceOf[ObjectNode])
    )
    metadataError
  }

  def jsonToObjectNode(nodeJson: String): ObjectNode =
    objectMapper.readTree(nodeJson).asInstanceOf[ObjectNode]

  def jsonToNode[T <: JsonNode](nodeJson: String): T =
    objectMapper.readTree(nodeJson).asInstanceOf[T]


  implicit class MetadataWarningsExtensions(warnings: MetadataWarnings) {
    def containsWarningWith(expectedMessageFragment: String): Boolean = {
      warnings.exists(_.message.contains(expectedMessageFragment))
    }
  }
}
