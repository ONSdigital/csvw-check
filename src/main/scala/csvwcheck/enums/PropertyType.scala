package csvwcheck.enums

object PropertyType extends Enumeration {
  val Context, Common, Inherited, Dialect, DataType, Table, TableGroup, Schema, ForeignKey, Column,
  Transformation, ForeignKeyReference, Annotation, Undefined = Value
}
