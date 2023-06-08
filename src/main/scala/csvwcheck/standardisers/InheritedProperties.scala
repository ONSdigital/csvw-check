package csvwcheck.standardisers

import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, NullNode, ObjectNode, TextNode}
import csvwcheck.enums.PropertyType
import csvwcheck.standardisers.Utils.{JsonNodeParser, MetadataErrorsOrParsedArrayElements, invalidValueWarning}
import csvwcheck.traits.ObjectNodeExtentions.ObjectNodeGetMaybeNode
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object InheritedProperties {
  val parsers: Map[String, JsonNodeParser] = Map(
    // Inherited properties
    "aboutUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "datatype" -> DataTypeProperties.parseDataTypeProperty(PropertyType.Inherited),
    "default" -> Utils.parseStringProperty(PropertyType.Inherited),
    "lang" -> Utils.parseLanguageProperty(PropertyType.Inherited),
    "null" -> parseNullProperty(PropertyType.Inherited),
    "ordered" -> Utils.parseBooleanProperty(PropertyType.Inherited),
    "propertyUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
    "required" -> Utils.parseBooleanProperty(PropertyType.Inherited),
    "separator" -> parseSeparatorProperty(PropertyType.Inherited),
    "textDirection" -> parseTextDirectionProperty(PropertyType.Inherited),
    "valueUrl" -> parseUriTemplateProperty(PropertyType.Inherited),
  )

  /**
    * Copy inherited properties from a parent object to a child object.
    * @param parentObject The parent object to inherit from
    * @param childObject The child object to copy to
    * @return
    */
  def copyInheritedProperties(parentObject: ObjectNode, childObject: ObjectNode): ObjectNode = {
    parsers.keys
      .foldLeft(childObject.deepCopy())({
        case (childObjectCopy, propertyName) =>
          // Copy over the inherited property from parent to child, if it exists on the parent.
          parentObject
            .getMaybeNode(propertyName)
            .foreach(valueToCopy => {
              childObject.set(propertyName, valueToCopy)
              ()
            })

          childObjectCopy
      })
  }

  private def parseUriTemplateProperty(
                                csvwPropertyType: PropertyType.Value
                              ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode => Right((s, Array[String](), csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode(""),
            Array[String](invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseTextDirectionProperty(
                                  csvwPropertyType: PropertyType.Value
                                ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s: TextNode
        if Array[String]("ltr", "rtl", "inherit").contains(s.asText()) =>
        Right((s, Array.empty, csvwPropertyType))
      case _ =>
        Right(
          (
            new TextNode("inherit"),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }

  }

  private def parseSeparatorProperty(
                              csvwPropertyType: PropertyType.Value
                            ): JsonNodeParser = { (value, _, _) => {
    value match {
      case s if s.isTextual || s.isNull =>
        Right((s, Array.empty, csvwPropertyType))
      case _ =>
        Right(
          (
            NullNode.getInstance(),
            Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }

  private def parseNullProperty(
                         csvwPropertyType: PropertyType.Value
                       ): JsonNodeParser = { (value, _, _) => {
    value match {
      case _: TextNode => Right((value, Array.empty, csvwPropertyType))
      case arrayNode: ArrayNode =>
        arrayNode
          .elements()
          .asScala
          .map({
            case element: TextNode => Right((Some(element), Array[String]()))
            case _ =>
              Right((None, Array(invalidValueWarning)))
          })
          .toArrayNodeAndStringWarnings
          .map(_ :+ csvwPropertyType)
      case _ =>
        Right(
          (
            JsonNodeFactory.instance.arrayNode().add(""),
            if (value.isNull) Array.empty
            else Array(invalidValueWarning),
            csvwPropertyType
          )
        )
    }
  }
  }
}
