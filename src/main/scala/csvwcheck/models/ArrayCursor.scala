/*
 * Copyright 2020 Crown Copyright (Office for National Statistics)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package csvwcheck.models

import scala.reflect.ClassTag

/**
  * A class to represent a cursor pointing to a particular value of an array. Expected to be used when parsing strings
  * or similar data-types.
  *
  * Contains helper functions designed to make it easier to walk/peek forwards and backwards through the array of
  * values.
  *
  * @param values - The values to iterate across.
  * @tparam T - Type of each item inside `values`.
  */
case class ArrayCursor[T: ClassTag](private val values: Seq[T]) {
  val arrayValues: Array[T] = Array.from[T](values)
  private var currentIndex: Int = -1

  def hasNext: Boolean = hasValue(1)

  /**
    * Whether or not the Array has a value `offset` items relative to the current index.
    *
    * @param offset - The offset from the current position at which to check for a value
    * @return Boolean
    */
  def hasValue(offset: Int): Boolean = {
    val offsetPosition = currentIndex + offset
    val maxIndex = arrayValues.length - 1
    maxIndex >= 0 && offsetPosition >= 0 && offsetPosition <= maxIndex
  }

  def hasPrevious: Boolean = hasValue(-1)

  /**
    * Set the index to `currentIndex + 1`
    *
    * @return the next value
    */
  def next(): T = {
    currentIndex += 1
    arrayValues(currentIndex)
  }

  /**
    * Set the index to `currentIndex - 1`
    */
  def stepBack(): Unit = currentIndex -= 1

  def peekNext(): T = peek(1)

  def peek(offset: Int): T = arrayValues(currentIndex + offset)

  def peekPrevious(): T = peek(-1)
}
