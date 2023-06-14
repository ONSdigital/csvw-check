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
