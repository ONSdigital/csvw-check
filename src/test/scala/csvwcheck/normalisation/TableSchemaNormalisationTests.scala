package csvwcheck.normalisation

import csvwcheck.enums.PropertyType
import NormalisationTestUtils.{assertJsonNodesEquivalent, jsonToObjectNode, starterContext}
import org.scalatest.funsuite.AnyFunSuite



class TableSchemaNormalisationTests extends AnyFunSuite {

    test("throw metadata error if @type of schema is not 'Schema'") {
      val tableSchemaNode = jsonToObjectNode("""
                   |{
                   | "@type": "someValueOtherThanSchema"
                   |}
                   |""".stripMargin)
      val Left(metadataError) = TableSchema.normaliseTableSchema(PropertyType.Undefined)(starterContext.withNode(tableSchemaNode))
      assert(
        metadataError.message == "@type must be 'Schema', found (someValueOtherThanSchema)"
      )
    }


    test("return expected schemaJson after validation") {
      val schemaNode = jsonToObjectNode("""
                   |{
                   | "@id": "https://chickenburgers.com",
                   | "separator": "separatorContent"
                   | }
                   |""".stripMargin)
      val Right((normalisedSchema, warnings, _)) = TableSchema.normaliseTableSchema(PropertyType.Undefined)(starterContext.withNode(schemaNode))

      assert(warnings.isEmpty)
      assertJsonNodesEquivalent(normalisedSchema, schemaNode) // separator property should Not be stripped of after validation as it comes under inherited
    }


}
