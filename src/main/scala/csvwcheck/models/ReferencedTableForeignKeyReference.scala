package csvwcheck.models

case class ReferencedTableForeignKeyReference(
                                               foreignKey: ForeignKeyDefinition,
                                               /**
                                                 * The table that the foreign key points to.
                                                 */
                                               referencedTable: Table,
                                               referencedTableReferencedColumns: Array[Column],
                                               /**
                                                 * The table the foreign key was defined on.
                                                 */
                                               definitionTable: Table
) {}
