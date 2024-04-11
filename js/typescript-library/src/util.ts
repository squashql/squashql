import {BinaryOperationField, Field, TableField} from "./field"
import PACKAGE from "./package"
import {ColumnSet, GroupColumnSet} from "./columnset"
import {Parameter, QueryCacheParameter} from "./parameter"
import {AggregatedMeasure, ComparisonMeasureReferencePosition, ParametrizedMeasure} from "./measure"
import {Criteria} from "./index"

export function serializeMap(map: Map<any, any>): Map<string, any> {
  const m = new Map()
  for (const [key, value] of map) {
    m.set(JSON.stringify(key), value)
  }
  return m
}

export function serializeRecord(r: Record<string, any>): Record<string, any> {
  const m = new Map()
  for (const [key, value] of Object.entries(r)) {
    m.set(key, JSON.stringify(value))
  }
  return m
}

function transformToObject(value: any): any {
  if (value === undefined) {
    return
  }

  const clazz = value["@class"]
  if (clazz === PACKAGE + "TableField") {
    return new TableField(value["fullName"], value["alias"])
  } else if (clazz === PACKAGE + "dto.GroupColumnSetDto") {
    const m: Map<string, Array<string>> = new Map
    value["values"] && Object.entries(value["values"])?.forEach(([k, v]) => m.set(k, transformToObject(v)))
    return new GroupColumnSet(value["newField"], value["field"], m)
  } else if (clazz === PACKAGE + "parameter.QueryCacheParameter") {
    return new QueryCacheParameter(value["action"])
  } else if (clazz === PACKAGE + "AggregatedMeasure") {
    return new AggregatedMeasure(value["alias"], transformToObject(value["field"]), value["aggregationFunction"], value["distinct"], value["criteria"])
  } else if (clazz === PACKAGE + "ComparisonMeasureReferencePosition") {
    const m: Map<Field, any> = new Map
    value["referencePosition"] && Object.entries(value["referencePosition"])?.forEach(([k, v]) => m.set(transformToObject(JSON.parse(k)), v))
    return new ComparisonMeasureReferencePosition(
            value["alias"],
            value["comparisonMethod"],
            transformToObject(value["measure"]),
            m.size == 0 ? undefined : m,
            value["columnSetKey"],
            value["elements"],
            value["period"],
            value["ancestors"]?.map((v: any) => transformToObject(v)),
            value["grandTotalAlongAncestors"])
  } else if (clazz === PACKAGE + "measure.ParametrizedMeasure") {
    return new ParametrizedMeasure(
            value["alias"],
            value["key"],
            value["parameters"])
  } else if (clazz === PACKAGE + "dto.CriteriaDto") {
    const c = value["children"]
    let children = undefined
    if (c) {
      children = []
      for (const cElement of c) {
        children.push(transformToObject(cElement))
      }
    }
    return new Criteria(transformToObject(value["field"]), transformToObject(value["fieldOther"]), transformToObject(value["measure"]), transformToObject(value["condition"]), value["conditionType"], children)
  } else if (clazz === PACKAGE + "BinaryOperationField") {
    return new BinaryOperationField(value["operator"], transformToObject(value["leftOperand"]), transformToObject(value["rightOperand"]), value["alias"])
  }
  return value
}

function reviver(key: string, value: any) {
  // console.log(`key = ${key}, value = ${value}`)
  if (key === "columnSets") {
    const m: Map<string, ColumnSet> = new Map
    Object.entries(value)?.forEach(([k, v]) => m.set(k, transformToObject(v)))
    return m
  } else if (key === "parameters") {
    const m: Map<string, Parameter> = new Map
    Object.entries(value)?.forEach(([k, v]) => m.set(k, transformToObject(v)))
    return m
  }

  if (typeof value === "object") {
    return transformToObject(value)
  } else {
    return value
  }
}

export function deserialize(value: string): any {
  return JSON.parse(value, reviver)
}
