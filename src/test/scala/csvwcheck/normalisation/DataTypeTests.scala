package csvwcheck.normalisation

import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.{MetadataWarningsExtensions, assertJsonNodesEquivalent, assertObjectNormalisation, assertObjectNormalisationFailure, jsonToObjectNode, starterContext}
import org.scalatest.funsuite.AnyFunSuite


class DataTypeTests extends AnyFunSuite {
  test(
      "datatype normaliser returns expected value for object with correct base and format"
  ) {
      val warnings = assertObjectNormalisation(
        DataType.normalisers,
       """
        {
          "base": "string",
          "format": "^The Sample RegEx$"
        }
      """,
        """
        {
          "base": "http://www.w3.org/2001/XMLSchema#string",
          "format": "^The Sample RegEx$"
        }
        """
      )

      assert(warnings.isEmpty)
    }

    test(
      "datatype normaliser returns expected value for object with invalid base and format"
    ) {
      val warnings = assertObjectNormalisation(
        DataType.normalisers,
        """
        {
          "base": "invalidDatatypeSupplied",
          "format": "^The Sample RegEx$"
        }
      """,
        """
        {
          "base": "http://www.w3.org/2001/XMLSchema#string",
          "format": "^The Sample RegEx$"
        }
        """
      )

      assert(warnings.containsWarningWith("invalid_datatype_base"))
    }


    test("datatype object cannot have @id of builtInDataTypes") {

      val metadataError = assertObjectNormalisationFailure(
        DataType.normalisers,
        """
        {
          "@id": "http://www.w3.org/2001/XMLSchema#string"
        }
      """
      )

      assert(
        metadataError.message === "datatype @id must not be the id of a built-in datatype (http://www.w3.org/2001/XMLSchema#string)"
      )
    }

    test(
      "datatype normaliser returns expected value for integer datatype"
    ) {
      val warnings = assertObjectNormalisation(
        DataType.normalisers,
        """
        {
          "base": "integer"
        }
      """,
        """
        {
          "base": "http://www.w3.org/2001/XMLSchema#integer"
        }
        """
      )

      assert(warnings.isEmpty)
    }

    test("Data type object with integer base cannot have length facet") {
      val nodeJson = """
        {
          "base": "integer",
          "length": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "Data types based on http://www.w3.org/2001/XMLSchema#integer cannot have a length facet")
    }

    test("datatype object with integer base cannot have minLength facet") {
      val nodeJson = """
        {
          "base": "integer",
          "minLength": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "Data types based on http://www.w3.org/2001/XMLSchema#integer cannot have a minLength facet")
    }

    test("datatype object with integer base cannot have maxLength facet") {
      val nodeJson = """
        {
          "base": "integer",
          "maxLength": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "Data types based on http://www.w3.org/2001/XMLSchema#integer cannot have a maxLength facet")
    }

    test(
      "datatype object with any base datatype other than numeric or datetime cannot have minInclusive"
    ) {
      val nodeJson = """
        {
          "base": "string",
          "minInclusive": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types")
    }

    test(
      "datatype object with any base datatype other than numeric or datetime cannot have maxExclusive"
    ) {
      val nodeJson = """
        {
          "base": "string",
          "maxExclusive": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "minimum/minInclusive/minExclusive/maximum/maxInclusive/maxExclusive are only allowed for numeric, date/time and duration types")
    }
    // 2 more similar tests for minExclusive and maxInclusive can be added

    test("datatype object cannot specify both minInclusive and minExclusive") {
      val nodeJson = """
        {
          "base": "decimal",
          "minInclusive": 10,
          "minExclusive": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(metadataError.message == "datatype cannot specify both minimum/minInclusive (10) and minExclusive (10)")
    }

  test("datatype object cannot specify both maxInclusive and maxExclusive") {
    val nodeJson = """
        {
          "base": "decimal",
          "maxInclusive": 10,
          "maxExclusive": 10
        }
      """

    val node = jsonToObjectNode(nodeJson)
    val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

    assert(metadataError.message == "datatype cannot specify both maximum/maxInclusive (10) and maxExclusive (10)")
  }

    test(
      "datatype object cannot specify both minInclusive greater than maxInclusive"
    ) {
      val nodeJson = """
        {
          "base": "decimal",
          "minInclusive": 10,
          "maxInclusive": 9
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(
        metadataError.message === "datatype minInclusive (10) cannot be greater than maxInclusive (9)"
      )
    }

    test("datatype minInclusive cannot be more than or equal to maxExclusive") {
      val nodeJson = """
        {
          "base": "decimal",
          "minInclusive": 10,
          "maxExclusive": 10
        }
      """

      val node = jsonToObjectNode(nodeJson)
      val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(node))

      assert(
        metadataError.message == "datatype minInclusive (10) cannot be greater than or equal to maxExclusive (10)"
      )
    }

    test("datatype check invalid min/max ranges throw exception") {
      val jsonArray = Array[String](
        """
          |{
          | "base": "decimal",
          | "minInclusive": 10,
          | "maxExclusive": 10
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "minInclusive": 10,
          | "maxInclusive": 9
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "minInclusive": 10,
          | "minExclusive": 10
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "minExclusive": 11,
          | "maxExclusive": 10
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "minExclusive": 11,
          | "maxInclusive": 10
          |}
          |""".stripMargin
      )

      for (json <- jsonArray) {
        val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(jsonToObjectNode(json)))
        assert(!metadataError.message.isEmpty)
      }
    }

    test("datatype check invalid min/max lengths throw exception") {
      val jsonArray = Array[String](
        """
          |{
          | "base": "decimal",
          | "minLength": 10,
          | "length": 9
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "maxLength": 8,
          | "length": 9
          |}
          |""".stripMargin,
        """
          |{
          | "base": "decimal",
          | "maxLength": 10,
          | "length": 9
          |}
          |""".stripMargin
      )

      for (json <- jsonArray) {
        val Left(metadataError) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(jsonToObjectNode(json)))
        assert(!metadataError.message.isEmpty)
      }
    }


    test("returns invalid boolean format warning if wrong boolean type format") {
      val json = """
                   |{
                   | "format": "YES|NO|MAYBE",
                   | "base": "boolean"
                   |}
                   |""".stripMargin
      val Right((_, warnings, _)) = DataType.normaliseDataType(PropertyType.Undefined)(
        starterContext.withNode(jsonToObjectNode(json))
      )
      assert(
        warnings.containsWarningWith("invalid_boolean_format '\"YES|NO|MAYBE\"'")
      )
    }

    // Format for boolean datatype is not being split into parts and kept in array anymore. Commenting this test for now
    test("returns expected object node when valid boolean type format supplied") {
      val dataTypeNode = jsonToObjectNode("""
                   |{
                   | "format": "YES|NO",
                   | "base": "boolean"
                   |}
                   |""".stripMargin)
      val Right((normalisedValue, warnings, _)) = DataType.normaliseDataType(PropertyType.Undefined)(
        starterContext.withNode(dataTypeNode)
      )

      assertJsonNodesEquivalent(normalisedValue, jsonToObjectNode(
        """
          |{
          | "format": "YES|NO",
          | "base": "http://www.w3.org/2001/XMLSchema#boolean"
          |}
          |""".stripMargin))


      assert(warnings.isEmpty)
    }

    test(
      "returns expected object node when valid regexp datatype is supplied in base"
    ) {
      val dataTypeNode = jsonToObjectNode("""
                   |{
                   | "format": "0xabcdefg",
                   | "base": "hexBinary"
                   |}
                   |""".stripMargin)

      val Right((normalisedValue, warnings, _)) = DataType.normaliseDataType(PropertyType.Undefined)(starterContext.withNode(dataTypeNode))

      assert(warnings.isEmpty)
      assertJsonNodesEquivalent(normalisedValue, jsonToObjectNode(
        """
          |{
          | "format": "0xabcdefg",
          | "base": "http://www.w3.org/2001/XMLSchema#hexBinary"
          |}
          |""".stripMargin))
    }

    test(
      "returns json with valid format and zero warnings for correct dateTime format"
    ) {
      val dataTypeNode = jsonToObjectNode("""
                   |{
                   | "format": "dd/MM/yyyy",
                   | "base": "date"
                   |}
                   |""".stripMargin)

      val Right((normalisedValue, warnings, _)) = DataType.normaliseDataType(PropertyType.Undefined)(
        starterContext.withNode(dataTypeNode)
      )

      assert(warnings.isEmpty)
      assertJsonNodesEquivalent(normalisedValue, jsonToObjectNode(
        """
          |{
          | "format": "dd/MM/yyyy",
          | "base": "http://www.w3.org/2001/XMLSchema#date"
          |}
          |""".stripMargin))
    }

    test(
      "returns json with format removed and appropriate warnings for incorrect dateTime format"
    ) {
      val dataTypeNode = jsonToObjectNode("""
                                            |{
                                            | "format": "dd/ZZ/yyyy",
                                            | "base": "date"
                                            |}
                                            |""".stripMargin)

      val Right((normalisedValue, warnings, _)) = DataType.normaliseDataType(PropertyType.Undefined)(
        starterContext.withNode(dataTypeNode)
      )

      assertJsonNodesEquivalent(normalisedValue, jsonToObjectNode(
        """
          |{
          | "base": "http://www.w3.org/2001/XMLSchema#date"
          |}
          |""".stripMargin))

      assert(warnings.containsWarningWith("invalid_date_format 'dd/ZZ/yyyy'"))
    }

}
