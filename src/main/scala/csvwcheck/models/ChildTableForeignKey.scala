package csvwcheck.models

import com.fasterxml.jackson.databind.node.ObjectNode

case class ChildTableForeignKey(
    jsonObject: ObjectNode,
    localColumns: Array[Column]
) {}
