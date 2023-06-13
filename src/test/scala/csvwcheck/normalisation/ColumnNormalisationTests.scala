package csvwcheck.normalisation

import NormalisationTestUtils.MetadataWarningsExtensions
import csvwcheck.normalisation.NormalisationTestUtils.assertObjectNormalisation
import org.scalatest.funsuite.AnyFunSuite


class ColumnNormalisationTests extends AnyFunSuite {

    // Title Property tests
    test("set lang object when value is textual in title property") {
      val warnings = assertObjectNormalisation(
        Column.normalisers,
        """
            |{
            | "titles": "Sample Title"
            |}
            |""".stripMargin,
        """
          |{
          | "titles": {
          |   "und": ["Sample Title"]
          | }
          |}
          |""".stripMargin
      )

      assert(warnings.isEmpty)
    }

    test(
      "set correct lang object and warnings when title property contains an array"
    ) {
      val warnings = assertObjectNormalisation(
        Column.normalisers,
        """
          |{
          | "titles": [true, "Sample Title"]
          |}
          |""".stripMargin,
        """
          |{
          | "titles": {
          |   "und": ["Sample Title"]
          | }
          |}
          |""".stripMargin
      )

      assert(warnings.containsWarningWith("[ true, \"Sample Title\" ] is invalid, textual elements expected"))
    }

    test(
      "set correct lang object and warnings when title property is an object"
    ) {

      val warnings = assertObjectNormalisation(
        Column.normalisers,
        """
          |{
          | "titles": {
          |   "sgn-BE-FR": "Some title",
          |   "Invalid Language Property": "Nooo"
          | }
          |}
          |""".stripMargin,
        """
          |{
          | "titles": {
          |   "sgn-BE-FR": ["Some title"]
          | }
          |}
          |""".stripMargin
      )

      assert(warnings.containsWarningWith("invalid_language"))
    }

}
