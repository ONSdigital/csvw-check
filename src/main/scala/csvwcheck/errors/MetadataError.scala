package csvwcheck.errors

case class MetadataError(
    message: String = "",
    cause: Throwable = None.orNull
) extends Exception(message, cause)
