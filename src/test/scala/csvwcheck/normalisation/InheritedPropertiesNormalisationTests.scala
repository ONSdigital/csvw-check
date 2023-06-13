package csvwcheck.normalisation

import NormalisationTestUtils.assertObjectNormalisation
import org.scalatest.funsuite.AnyFunSuite
import NormalisationTestUtils.MetadataWarningsExtensions


class InheritedPropertiesNormalisationTests extends AnyFunSuite {
  test("null property returns value in array without warnings on valid value") {
    val warnings = assertObjectNormalisation(
      InheritedProperties.normalisers,
      """
        {
          "null": "x"
        }
      """,
      """
        {
          "null": ["x"]
        }
      """
    )

    assert(warnings.isEmpty, warnings)
  }

  test("null property returns warnings for invalid value (non string type)") {
    val warnings = assertObjectNormalisation(
      InheritedProperties.normalisers,
      """
        {
          "null": false
        }
      """,
      """
        {
          "null": [""]
        }
      """
    )

    assert(warnings.containsWarningWith("invalid_value"))
  }

  test("null property with values array holding different types") {
    val warnings = assertObjectNormalisation(
      InheritedProperties.normalisers,
      """
        {
          "null": ["sample", 5]
        }
      """,
      """
        {
          "null": ["sample"]
        }
      """
    )

    assert(warnings.containsWarningWith("invalid_value"))
  }

  test(
    "separator property returns invalid warning if not of type string or null"
  ) {
    val warnings = assertObjectNormalisation(
      InheritedProperties.normalisers,
      """
        {
          "separator": false
        }
      """,
      """
        {
          "separator": null
        }
      """
    )

    assert(warnings.containsWarningWith("invalid_value"))  }

  test("separator property returns value on valid case") {
    val warnings = assertObjectNormalisation(
      InheritedProperties.normalisers,
      """
        {
          "separator": ","
        }
      """,
      """
        {
          "separator": ","
        }
      """
    )

    assert(warnings.isEmpty, warnings)
  }


  test("set invalid value warnings when Uri template property is valid") {
      val warnings = assertObjectNormalisation(
        InheritedProperties.normalisers,
        """
          |{
          | "propertyUrl": "https://example.com/{+something}"
          |}
          |""".stripMargin,
        """
          |{
          | "propertyUrl": "https://example.com/{+something}"
          |}
          |""".stripMargin
      )

      assert(warnings.isEmpty)
    }

  test("set value and warnings correctly when Uri template property is not string") {
      val warnings = assertObjectNormalisation(
        InheritedProperties.normalisers,
        """
          |{
          | "propertyUrl": 3.5
          |}
          |""".stripMargin,
        """
          |{
          | "propertyUrl": ""
          |}
          |""".stripMargin
      )

      assert(warnings.containsWarningWith("invalid_value"))
    }


    test(
      "set correct value and warnings correctly when textDirection property is valid"
    ) {
      val warnings = assertObjectNormalisation(
        InheritedProperties.normalisers,
        """
          |{
          | "textDirection": "rtl"
          |}
          |""".stripMargin,
        """
          |{
          | "textDirection": "rtl"
          |}
          |""".stripMargin
      )

      assert(warnings.isEmpty)
    }

    test(
      "set correct value and warnings correctly when textDirection property is Invalid"
    ) {
      val warnings = assertObjectNormalisation(
        InheritedProperties.normalisers,
        """
          |{
          | "textDirection": "not a valid text direction"
          |}
          |""".stripMargin,
        """
          |{
          | "textDirection": "inherit"
          |}
          |""".stripMargin
      )

      assert(warnings.containsWarningWith("invalid_value"))
    }


}
