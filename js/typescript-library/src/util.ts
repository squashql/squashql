import {Field, TableField} from "./field";

export function toField(field: Field | string): Field {
    return typeof field === 'string' ? new TableField(field) : field
}

export function serializeMap(map: Map<any, any>): Map<string, any> {
    const m = new Map()
    for (let [key, value] of map) {
        m.set(JSON.stringify(key), value)
    }
    return m
}
