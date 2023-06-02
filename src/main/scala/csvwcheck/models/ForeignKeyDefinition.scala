package csvwcheck.models

import com.fasterxml.jackson.databind.node.ObjectNode

case class ForeignKeyDefinition(
    jsonObject: ObjectNode,
    localColumns: Array[Column]
) {
  override def toString: String =
    s"ForeignKeyDefinition([${localColumns.map(_.name.getOrElse("unnamed column")).mkString(", ")}])"

}
