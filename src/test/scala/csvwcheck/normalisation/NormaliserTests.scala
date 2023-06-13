package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.starterContext
import org.scalatest.funsuite.AnyFunSuite
import NormalisationTestUtils.MetadataWarningsExtensions

class NormaliserTests extends AnyFunSuite {
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




//
//  test("set invalid value warnings when Uri template property is not string") {
//    val json = """
//                 |{
//                 |  "sampleObjectProperty": "some content"
//                 | }
//                 |""".stripMargin
//    val jsonNode = objectMapper.readTree(json)
//    val Right((_, warnings, _)) = TableGroup.parseJsonProperty(
//      "propertyUrl",
//      jsonNode,
//      baseUrl = "https://chickenburgers.com",
//      "und"
//    )
//
//    assert(warnings === Array[String]("invalid_value"))
//  }
//
//  test("set value and warnings correctly when Uri template property valid") {
//    val validTextNodeUrl = new TextNode("https://www.w3.org")
//    val Right((values, warnings, _)) = TableGroup.parseJsonProperty(
//      "propertyUrl",
//      validTextNodeUrl,
//      baseUrl = "https://chickenburgers.com",
//      "und"
//    )
//
//    assert(values === validTextNodeUrl)
//    assert(warnings === Array[String]())
//  }
//
//  test(
//    "set correct value and warnings correctly when textDirection property is valid"
//  ) {
//    val validTextDirection = new TextNode("rtl")
//    val Right((values, warnings, _)) = TableGroup.parseJsonProperty(
//      "textDirection",
//      validTextDirection,
//      baseUrl = "https://www.w3.org",
//      "und"
//    )
//
//    assert(values === validTextDirection)
//    assert(warnings === Array[String]())
//  }
//
//  test(
//    "set correct value and warnings correctly when textDirection property is Invalid"
//  ) {
//    val validTextDirection =
//      new TextNode("Some value which is not a text direction")
//    val Right((_, warnings, _)) = TableGroup.parseJsonProperty(
//      "textDirection",
//      validTextDirection,
//      baseUrl = "https://www.w3.org",
//      "und"
//    )
//
//    assert(warnings === Array[String]("invalid_value"))
//  }
//
//  // Title Property tests
//  test("set lang object when value is textual in title property") {
//    val Right((values, warnings, _)) = TableGroup.parseJsonProperty(
//      "titles",
//      new TextNode("Sample Title"),
//      "",
//      "und"
//    )
//
//    assert(!values.path("und").isMissingNode)
//    assert(values.get("und").isArray)
//    assert(values.get("und").elements().next().asText() === "Sample Title")
//    assert(warnings === Array[String]())
//  }
//
//  test(
//    "set correct lang object and warnings when title property contains an array"
//  ) {
//    val arrNode = JsonNodeFactory.instance.arrayNode()
//    arrNode.add(true)
//    arrNode.add("sample text value")
//    val Right((values, warnings, _)) =
//      TableGroup.parseJsonProperty("titles", arrNode, "", "und")
//
//    assert(!values.path("und").isMissingNode)
//    assert(values.get("und").isArray)
//    assert(
//      values.get("und").elements().next().asText() === "sample text value"
//    )
//    assert(
//      warnings === Array[String](
//        "[ true, \"sample text value\" ] is invalid, textual elements expected"
//      )
//    )
//  }
//
//  test(
//    "set correct lang object and warnings when title property is an object"
//  ) {
//    val json =
//      """
//                 |{
//                 |  "invalidLanguageProperty": "sample", "sgn-BE-FR": "sample content"
//                 | }
//                 |""".stripMargin
//    val jsonNode = objectMapper.readTree(json)
//    val Right((values, warnings, _)) =
//      TableGroup.parseJsonProperty("titles", jsonNode, "", "und")
//    val expectedTitleArray =
//      JsonNodeFactory.instance.arrayNode().add("sample content")
//
//    assert(!values.path("sgn-BE-FR").isMissingNode)
//    assert(values.get("sgn-BE-FR").isArray)
//    assert(values.get("sgn-BE-FR") === expectedTitleArray)
//    assert(warnings === Array[String]("invalid_language"))
//  }
//
//  // Transformations Property tests
//  test(
//    "return the entire transformations without warnings when provided with valid transformations array"
//  ) {
//    val json =
//      """
//        | [{
//        |    "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
//        |    "titles": "Simple XML version",
//        |    "url": "xml-template.mustache",
//        |    "scriptFormat": "https://mustache.github.io/",
//        |    "source": "json"
//        |  }]
//        |""".stripMargin
//    val jsonNode = objectMapper.readTree(json)
//    val Right((values, warnings, _)) =
//      TableGroup.parseJsonProperty("transformations", jsonNode, "", "und")
//
//    assert(warnings === Array[String]())
//    assert(values === jsonNode)
//  }
//
//  test(
//    "return transformations after stripping off invalid transformation objects"
//  ) {
//    val json =
//      """
//        | [{
//        |    "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
//        |    "titles": "Simple XML version",
//        |    "url": "xml-template.mustache",
//        |    "scriptFormat": "https://mustache.github.io/",
//        |    "source": "json",
//        |    "textDirection": "Some value"
//        |  }]
//        |""".stripMargin
//    val jsonNode = objectMapper.readTree(json)
//    val Right((values, warnings, _)) =
//      TableGroup.parseJsonProperty("transformations", jsonNode, "", "und")
//
//    assert(
//      warnings.contains("invalid_property 'textDirection' with type Inherited")
//    )
//    assert(warnings.contains("invalid_value"))
//    assert(!values.asInstanceOf[ArrayNode].has("source"))
//    // After processing TextDirection will be removed from json or should not be present in json
//    assert(!values.asInstanceOf[ArrayNode].has("textDirection"))
//  }
//
//  test("throw exception when transformation objects cannot be processed") {
//    val json =
//      """
//        | [{
//        |    "targetFormat": "http://www.iana.org/assignments/media-types/application/xml",
//        |    "@id": "_: starts with Underscore colon which is not accepted",
//        |    "titles": "Simple XML version"
//        |  }]
//        |""".stripMargin
//    val jsonNode = objectMapper.readTree(json)
//    val Left(MetadataError(errorMessage, _)) =
//      TableGroup.parseJsonProperty("transformations", jsonNode, "", "und")
//
//    assert(errorMessage === "transformations[0].@id starts with _:")
//  }
//
//  test(
//    "should insert pattern key under format if format is textual for Numeric format datatypes"
//  ) {
//    // Update: Not validating numeric data-types with formats
////    val json =
////      """
////        |{
////        | "base": "decimal",
////        | "format": "0.###E0"
////        |}
////        |""".stripMargin
////
////    val jsonNode = objectMapper.readTree(json)
////    val (values, warnings, _) =
////      PropertyChecker.checkProperty("datatype", jsonNode, "", "und")
////
////    // format object should contain the key pattern
////    assert(!values.path("format").get("pattern").isMissingNode)
//  }
//
//  test("should populate warnings for invalid number format datatypes") {
//    // Update: Not validating numeric data-types with formats
////    val json =
////      """
////        |{
////        | "base": "decimal",
////        | "format": "0.#00#"
////        |}
////        |""".stripMargin
////
////    val jsonNode = objectMapper.readTree(json)
////    val (values, warnings, _) =
////      PropertyChecker.checkProperty("datatype", jsonNode, "", "und")
////
////    assert(values.path("format").path("pattern").isMissingNode)
////    assert(warnings.contains("invalid_number_format"))
////    assert(
////      warnings.contains(
////        "Malformed pattern for ICU DecimalFormat: \"0.#00#\": 0 cannot follow # after decimal point at position 3"
////      )
////    )
//  }
//
//  test(
//    "should populate warnings for invalid format for regex format datatypes"
//  ) {
//    val json =
//      """
//        |{
//        | "base": "yearMonthDuration",
//        | "format": "[("
//        |}
//        |""".stripMargin
//
//    val jsonNode = objectMapper.readTree(json)
//    val Right((_, warnings, _)) =
//      TableGroup.parseJsonProperty("datatype", jsonNode, "", "und")
//
//    assert(warnings(0).contains("invalid_regex"))
//  }
}
