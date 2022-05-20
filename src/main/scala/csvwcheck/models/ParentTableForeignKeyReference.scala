package csvwcheck.models

case class ParentTableForeignKeyReference(
    foreignKey: ChildTableForeignKey,
    parentTable: Table,
    parentTableReferencedColumns: Array[Column],
    childTable: Table
) {}
