package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, TextNode}
import csvwcheck.TestPaths.{fixturesPath, resourcesPath}
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.{MetadataWarningsExtensions, assertJsonNodesEquivalent, assertObjectNormalisation, assertObjectNormalisationFailure, jsonToNode, jsonToObjectNode, starterContext}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.IteratorHasAsScala

class ForeignKeyNormalisationTests extends AnyFunSuite {

    // ForeignKeys property tests
    test(
      "throw metadata error when property foreignKey property value contains colon"
    ) {
      val foreignKeyArray = jsonToNode[ArrayNode]("""
        |[{
        | "@id": "https://chickenburgers.com",
        | "contain:colon": "separatorContent"
        |}]
        |""".stripMargin)

      val Left(metadataWarning) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeyArray))

      assert(
        metadataWarning.message.contains("foreignKey includes a prefixed (common) property")
      )

    }

    test(
      "return invalid value warning if foreignKeys property value is not array"
    ) {
      val foreignKeyObject = jsonToObjectNode("""
                   |{
                   | "@id": "https://chickenburgers.com"
                   | }
                   |""".stripMargin)
      val Right((normalisedValue, warnings, _)) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeyObject))

      assert(warnings.containsWarningWith("invalid_value"))
      assertJsonNodesEquivalent(normalisedValue, JsonNodeFactory.instance.arrayNode(0))
    }

    test(
      "return correct jsonNode with property removed and warnings if property is not valid"
    ) {
      val foreignKeysArray = jsonToNode[ArrayNode](
        """
          |[
          |{"datatype": "invalidTextDataSupplied"}
          |]
          |""".stripMargin
      )
      val Right((normalisedValue, warnings, _)) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeysArray))

      assert(warnings.containsWarningWith("invalid_value"))
      assertJsonNodesEquivalent(normalisedValue, jsonToNode[ArrayNode]("[{}]"))
    }


    // Reference Property tests
    test("load reference from a separate document") {
      val testCaseFileUri = resourcesPath
        ./("csvwExamples")
        ./("foreignKeyResourceExample.json")
        .path
        .toAbsolutePath
        .toUri

      val foreignKeysArray = jsonToNode[ArrayNode](
        s"""
          |[
          | {
          |   "reference": "$testCaseFileUri"
          | }
          |]
          |""".stripMargin
      )
      val Right((normalisedValue, warnings, _)) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeysArray))

      assert(warnings.isEmpty)
      val expectedNormalisedNode = jsonToNode[ArrayNode](
        """
          |[
          | {
          |   "reference": {
          |     "resource": "https://example.com/something.csv",
          |     "columnReference": ["Some Column"]
          |   }
        |   }
          |]
          |""".stripMargin)

      assertJsonNodesEquivalent(normalisedValue, expectedNormalisedNode)
    }

    test(
      "throw metadata error when property reference property value contains common property"
    ) {
      val foreignKeysArray = jsonToNode[ArrayNode]("""
                   |[
                   |  {
                   |    "reference": {
                   |      "resource": "something.csv",
                   |      "columnReference": "Some Column",
                   |      "contain:colon": "some content"
                   |    }
                   |  }
                   |]
                   |""".stripMargin)

      val Left(metadataError) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeysArray))

      assert(metadataError.message.contains("foreignKey reference (contain:colon) includes a prefixed (common) property"))
    }

    test(
      "error when reference is missing columnReference"
    ) {
      val foreignKeysArray = jsonToNode[ArrayNode]("""
                                                     |[
                                                     |  {
                                                     |    "reference": {
                                                     |      "resource": "something.csv"
                                                     |    }
                                                     |  }
                                                     |]
                                                     |""".stripMargin)

      val Left(metadataError) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeysArray))

      assert(metadataError.message.contains("foreignKey reference columnReference is missing"))
    }


  test(
    "error when reference is missing a resource or schemaReference"
  ) {
    val foreignKeysArray = jsonToNode[ArrayNode]("""
                                                   |[
                                                   |  {
                                                   |    "reference": {
                                                   |      "columnReference": "Some Column"
                                                   |    }
                                                   |  }
                                                   |]
                                                   |""".stripMargin)

    val Left(metadataError) = ForeignKey.normaliseForeignKeysProperty(PropertyType.Undefined)(starterContext.withNode(foreignKeysArray))

    assert(metadataError.message.contains("foreignKey reference does not have either resource or schemaReference"))
  }
    // Add more test cases for referenceProperty after resource, schemaReference, columnReference property validations
    // are implemented. Currently the exceptions raised when these properties are missing is not tested since
    // NoSuchElementException is thrown when a jsonnode with these properties are passed in.

}
