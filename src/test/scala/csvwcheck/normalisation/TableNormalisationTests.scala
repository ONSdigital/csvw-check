package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.NormalisationTestUtils.starterContext
import org.scalatest.funsuite.AnyFunSuite
import NormalisationTestUtils.MetadataWarningsExtensions

import scala.jdk.CollectionConverters.IteratorHasAsScala

class TableNormalisationTests extends AnyFunSuite {
    test(
      "notes normaliser should return invalid if value is not of type array"
    ) {
      val Right((value, warnings, _)) = Table.normaliseNotesProperty(PropertyType.Undefined)(
        starterContext.withNode(new TextNode("Invalid notes property"))
      )

      assert(warnings.containsWarningWith("invalid_value"))
      assert(value.isInstanceOf[ArrayNode])
    }

    test(
      "notes normaliser returns values, warnings array for valid input"
    ) {
      //    val notesArray = Array("firstNote", "SecondNote", "ThirdNote")
      val arrayNode = JsonNodeFactory.instance.arrayNode()
      arrayNode.add("FirstNote")
      arrayNode.add("secondNote") // Find a better way initialize ArrayNode
      val Right((values, warnings, _)) = Table.normaliseNotesProperty(PropertyType.Undefined)(
        starterContext.withNode(arrayNode)
      )

      assert(values.isInstanceOf[ArrayNode])
      val arrayNodeOut = values.asInstanceOf[ArrayNode]
      val notes = Array.from(arrayNodeOut.elements.asScala)
      assert(notes(0).isInstanceOf[TextNode])
      assert(notes(0).asText() == "FirstNote")
      assert(notes(1).isInstanceOf[TextNode])
      assert(notes(1).asText() == "secondNote")
    }


}
