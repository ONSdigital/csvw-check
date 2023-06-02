package csvwcheck.traits

import csvwcheck.models.ParseResult.ParseResult
import shapeless.ops.tuple

object ParseResultExtensions {

  implicit class ParseResultExtensions[T1](parseResult: ParseResult[T1]) {

    /**
      * flatMap to another ParseResult[T2] and return both results  in a tuple `(result1: T1, result2: T2)`.
      * @param mapToNextParseResult
      * @tparam T2
      * @return
      */
    def flatMapStartAccumulating[T2](
        mapToNextParseResult: T1 => ParseResult[T2]
    ): ParseResult[(T1, T2)] = {
      parseResult.flatMap(t1Value =>
        mapToNextParseResult(t1Value)
          .map(t2Value => (t1Value, t2Value))
      )
    }
  }

  implicit class TupleParseResultExtensions[T1](parseResult: ParseResult[T1]) {

    /**
      * flatMap to another ParseResult[T2] and append the new result to the existing results tuple `(result1, ..., newResult: T2)`.
      * @param mapToNextParseResult
      * @param prepend
      * @tparam T2
      * @return
      */
    def flatMapKeepAccumulating[T2](
        mapToNextParseResult: T1 => ParseResult[T2]
    )(implicit
        prepend: tuple.Prepend[T1, Tuple1[T2]]
    ): ParseResult[prepend.Out] = {
      parseResult.flatMap(t1Value =>
        mapToNextParseResult(t1Value)
          .map(t2Value => prepend(t1Value, Tuple1(t2Value)))
      )
    }
  }
}
