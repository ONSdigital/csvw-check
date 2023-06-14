package csvwcheck.errors

case class MetadataError(
                          message: String = "",
                          propertyPath: Array[String] = Array(),
                          cause: Throwable = None.orNull
                        ) extends Exception(message, cause)
