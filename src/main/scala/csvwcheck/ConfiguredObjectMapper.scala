package csvwcheck

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

object ConfiguredObjectMapper {
  val objectMapper = new ObjectMapper()
  objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
  objectMapper.configure(
    DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS,
    true
  )
}
