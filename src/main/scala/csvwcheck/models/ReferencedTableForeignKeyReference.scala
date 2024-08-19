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

case class ReferencedTableForeignKeyReference(
                                               foreignKeyDefinition: ForeignKeyDefinition,

                                               /**
                                                 * The table that the foreign key points to.
                                                 */
                                               referencedTable: Table,
                                               referencedTableReferencedColumns: Array[Column],

                                               /**
                                                 * The table the foreign key was defined on.
                                                 */
                                               definitionTable: Table
                                             ) {
  override def toString: String =
    s"ReferencedTableForeignKeyReference($definitionTable.[${
      foreignKeyDefinition.localColumns
        .map(_.name.getOrElse("unnamed column"))
        .mkString(", ")
    }] -> $referencedTable.[${
      referencedTableReferencedColumns
        .map(_.name.getOrElse("unnamed column"))
        .mkString(",")
    }])"
}
