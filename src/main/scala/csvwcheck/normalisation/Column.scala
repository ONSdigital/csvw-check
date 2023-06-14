package csvwcheck.normalisation

import com.fasterxml.jackson.databind.node._
import csvwcheck.enums.PropertyType
import csvwcheck.normalisation.RegExpressions.NameRegExp
import csvwcheck.normalisation.Utils.{Normaliser, invalidValueWarning, noWarnings}
import shapeless.syntax.std.tuple.productTupleOps

object Column {
  val normalisers: Map[String, Normaliser] = Map(
    // https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#h-columns
    "@type" -> Utils.normaliseRequiredType(PropertyType.Common, "Column"),
    // Column level properties
    "name" -> normaliseNameProperty(PropertyType.Column),
    "suppressOutput" -> Utils.normaliseBooleanProperty(PropertyType.Column),
    "titles" -> Utils.normaliseNaturalLanguageProperty(PropertyType.Column),
    "virtual" -> Utils.normaliseBooleanProperty(PropertyType.Column),
  ) ++ InheritedProperties.normalisers ++ IdProperty.normaliser


  def normaliseColumn(propertyType: PropertyType.Value): Normaliser = {
    case context => context.node match {
      case columnNode: ObjectNode =>
        Utils.normaliseObjectNode(normalisers, context.withNode(columnNode))
          .map(_ :+ propertyType)
      case columnNode =>
        // Any items within an array that are not valid objects of the type expected are ignored
        Right(
          (
            NullNode.getInstance(),
            Array(context.makeWarning(s"Unexpected column value: ${columnNode.toPrettyString}")),
            propertyType
          )
        )
    }
  }


  private def normaliseNameProperty(
                                     csvwPropertyType: PropertyType.Value
                                   ): Normaliser = {
    case context =>
      context.node match {
        case s: TextNode =>
          if (NameRegExp.matches(s.asText())) {
            Right((s, noWarnings, csvwPropertyType))
          } else {
            Right(
              (
                NullNode.instance,
                Array(context.makeWarning(invalidValueWarning)),
                csvwPropertyType
              )
            )
          }
        case _ =>
          Right(
            (
              NullNode.getInstance,
              Array(context.makeWarning(invalidValueWarning)),
              csvwPropertyType
            )
          )
      }
  }


}
