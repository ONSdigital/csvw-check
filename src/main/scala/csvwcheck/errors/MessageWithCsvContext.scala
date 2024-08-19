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

package csvwcheck.errors

/**
  * Represents an abstract Error/Warning to be displayed to the user.
  */
abstract class MessageWithCsvContext {
  def `type`: String

  def category: String

  def row: String

  def column: String

  def content: String

  def constraints: String
}

case class ErrorWithCsvContext(
                                `type`: String,
                                category: String,
                                row: String,
                                column: String,
                                content: String,
                                constraints: String
                              ) extends MessageWithCsvContext {}

case class WarningWithCsvContext(
                                  `type`: String,
                                  category: String,
                                  row: String,
                                  column: String,
                                  content: String,
                                  constraints: String
                                ) extends MessageWithCsvContext {}
