package csvwcheck.normalisation

import csvwcheck.normalisation.NormalisationTestUtils.assertObjectNormalisationFailure
import org.scalatest.funsuite.AnyFunSuite


class IdPropertyNormalisationTests extends AnyFunSuite {
    test("throw metadata error if id starts with _:") {
      val metadataError = assertObjectNormalisationFailure(
        IdProperty.normaliser,
        """
          |{
          | "@id": "_:someValue"
          |}
          |""".stripMargin
      )
      assert(metadataError.message == "'_:someValue' starts with _:")
    }
}
