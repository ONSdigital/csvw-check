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

}
