package csvwcheck.models

import csvwcheck.errors.MetadataError

object ParseResult {
  type ParseResult[T] = Either[MetadataError, T]
}
