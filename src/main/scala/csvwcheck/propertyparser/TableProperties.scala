package csvwcheck.propertyparser

import com.fasterxml.jackson.databind.node._
import csvwcheck.ConfiguredObjectMapper.objectMapper
import csvwcheck.enums.PropertyType
import csvwcheck.models.ParseResult.ParseResult
import csvwcheck.propertyparser.RegExpressions.Bcp47LanguagetagRegExp
import csvwcheck.propertyparser.Utils.{JsonNodeParser, MetadataErrorsOrParsedObjectProperties, StringWarnings, invalidValueWarning}
import shapeless.syntax.std.tuple.productTupleOps

import scala.jdk.CollectionConverters.IteratorHasAsScala

object TableProperties {

}
