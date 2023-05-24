package csvwcheck.models

import com.fasterxml.jackson.databind.node.ObjectNode

case class ForeignKeyDefinition(
    jsonObject: ObjectNode,
    localColumns: Array[Column]
) {}
