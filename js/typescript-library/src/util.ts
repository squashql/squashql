import {Field, TableField} from "./field";

export function toField(field: Field | string): Field {
  return typeof field === 'string' ? new TableField(field) : field
}
