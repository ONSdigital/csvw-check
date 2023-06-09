package csvwcheck.models

case class ValidateRowOutput(
                              recordNumber: Long,
                              warningsAndErrors: WarningsAndErrors = WarningsAndErrors(),
                              primaryKeyValues: List[Any] = List(),
                              parentTableForeignKeyReferences: Map[
                                ReferencedTableForeignKeyReference,
                                KeyWithContext
                              ] = Map(),
                              childTableForeignKeys: Map[ForeignKeyDefinition, KeyWithContext] = Map()
                            )
