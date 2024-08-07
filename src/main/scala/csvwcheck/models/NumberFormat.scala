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

import com.ibm.icu.text.DecimalFormat
import csvwcheck.errors.MetadataError

case class NumberFormat(
                         pattern: Option[String],
                         groupChar: Option[Char] = None,
                         decimalChar: Option[Char] = None
                       ) {

  /**
    * Raising an exception when a pattern is supplied as we cannot figure out how to actually use them in some scenarios.
    */
  if (pattern.isDefined)
    throw MetadataError("Format supplied for numeric data-types")

  private val df: DecimalFormat = new DecimalFormat()
  private val decimalFormatSymbols = df.getDecimalFormatSymbols

  decimalFormatSymbols.setInfinity("INF")
  try {
    decimalChar match {
      case Some(c) => decimalFormatSymbols.setDecimalSeparator(c)
      // If no decimal separator is specified, set it to . (dot)
      // Setting it since we are not sure of the locale the user running this program will have, and might cause issues
      // if it is not the default decimal separator .
      case _ => decimalFormatSymbols.setDecimalSeparator('.')
    }
    groupChar match {
      case Some(c) => decimalFormatSymbols.setGroupingSeparator(c)
      // If no grouping separator is specified, set it to , (comma)
      case _ => decimalFormatSymbols.setGroupingSeparator(',')
    }
    df.setDecimalFormatSymbols(decimalFormatSymbols)
    pattern match {
      case Some(p) =>
        df.applyPattern(p)
        df.setParseStrict(true)
      case _ =>
      // Figure out what the default pattern should be
    }
  } catch {
    case e: Exception => throw MetadataError(e.getMessage, cause = e)
  }

  def parse(value: String): Number = {
    df.parse(value)
  }

  def format(value: Number): String = {
    df.format(value)
  }
}
