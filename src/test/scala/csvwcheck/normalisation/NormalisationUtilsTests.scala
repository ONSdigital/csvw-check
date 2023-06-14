package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.starterContext
import org.scalatest.funsuite.AnyFunSuite
import NormalisationTestUtils.MetadataWarningsExtensions

class NormalisationUtilsTests extends AnyFunSuite {
  test(
    "boolean normaliser should return invalid for non boolean values"
  ) {
    val Right((value, warnings, _)) = Utils.normaliseBooleanProperty(PropertyType.Undefined)(
      starterContext.withNode(new TextNode("Not a boolean"))
    )

    assert(warnings.containsWarningWith("invalid_value"))
    assert(value === BooleanNode.getFalse)
  }

  test(
    "boolean normaliser should return zero warnings for boolean values"
  ) {
    val Right((value, warnings, _)) = Utils.normaliseBooleanProperty(PropertyType.Undefined)(
      starterContext.withNode(BooleanNode.getTrue)
    )
    assert(value === BooleanNode.getTrue)
    assert(warnings.isEmpty)
  }

  test(
    "language normaliser should return invalid for properties which cannot be accepted"
  ) {
    val Right((_, warnings, _)) = Utils.normaliseLanguageProperty(PropertyType.Undefined)(
      starterContext.withNode(new TextNode("Invalid Language Property"))
    )

    assert(warnings.containsWarningWith("invalid_value"))
  }

  test(
    "language normaliser should return no warnings for correct language property"
  ) {
    val Right((_, warnings, _)) = Utils.normaliseLanguageProperty(PropertyType.Undefined)(
      starterContext.withNode(new TextNode("sgn-BE-FR"))
    )
    assert(warnings.isEmpty)
  }


  test(
    "link normaliser should return joined url after validation if baseUrl supplied"
  ) {
    val Right((value, _, _)) = Utils.normaliseUrlLinkProperty(PropertyType.Undefined)(
      starterContext.withNode(new TextNode("relative-path"))
    )
    assert(value === new TextNode(s"${starterContext.baseUrl}relative-path"))
  }

  test(
    "link normaliser should return value as url after validation if baseUrl not supplied"
  ) {
    val Right((value, _, _)) = Utils.normaliseUrlLinkProperty(PropertyType.Undefined)(
      starterContext.copy(node = new TextNode("relative-path"), baseUrl = "")
    )
    assert(value === new TextNode("relative-path"))
  }

  test(
    "string normaliser returns invalid warning if passed value is not string"
  ) {
    val Right((_, warnings, _)) = Utils.normaliseStringProperty(PropertyType.Undefined)(starterContext.withNode(BooleanNode.getFalse))
    assert(warnings.containsWarningWith("invalid_value"))
  }

  test(
    "string normaliser returns string value without warnings if passed value is string"
  ) {
    val Right((value, warnings, _)) = Utils.normaliseStringProperty(PropertyType.Undefined)(starterContext.withNode(new TextNode("sample string")))
    assert(warnings.isEmpty)
    assert(value === new TextNode("sample string"))
  }

  test("numeric property returns invalid on negative values") {
    val Right((_, warnings, _)) = Utils.normaliseNonNegativeIntegerProperty(PropertyType.Undefined)(starterContext.withNode(new IntNode(-10)))
    assert(warnings.containsWarningWith("invalid_value"))
  }

  test("numeric property returns value without warnings on valid value") {
    val Right((value, warnings, _)) = Utils.normaliseNonNegativeIntegerProperty(PropertyType.Undefined)(starterContext.withNode(new IntNode(5)))

    assert(warnings.isEmpty)
    assert(value === new IntNode(5))
  }
}
