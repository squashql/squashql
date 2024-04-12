import {AliasedField, BinaryOperationField, ConstantField, Field, TableField} from "./field"
import PACKAGE from "./package"
import {ColumnSet, GroupColumnSet} from "./columnset"
import {Parameter, QueryCacheParameter} from "./parameter"
import {
  AggregatedMeasure,
  BinaryOperationMeasure,
  ComparisonMeasureGrandTotal,
  ComparisonMeasureReferencePosition,
  DoubleConstantMeasure,
  ExpressionMeasure,
  LongConstantMeasure,
  ParametrizedMeasure,
  PartialHierarchicalComparisonMeasure
} from "./measure"
import {ConstantCondition, InCondition, LogicalCondition, SingleValueCondition} from "./condition"
import Criteria from "./criteria"
import {Month, Quarter, Semester, Year} from "./period"
import {ExplicitOrder, Order, SimpleOrder} from "./order"

export function serializeMap(map: Map<any, any>): Map<string, any> {
  const m = new Map()
  for (const [key, value] of map) {
    m.set(JSON.stringify(key), value)
  }
  return m
}

function transformToObject(key: string, value: any, reviverFallback?: (key: string, value: any) => any): any {
  if (value === undefined) {
    return
  }

  const clazz = value["@class"]
  if (clazz === PACKAGE + "TableField") {
    return new TableField(value["fullName"], value["alias"])
  } else if (clazz === PACKAGE + "AliasedField") {
    return new AliasedField(value["alias"])
  } else if (clazz === PACKAGE + "dto.GroupColumnSetDto") {
    const m: Map<string, Array<string>> = new Map
    value["values"] && Object.entries(value["values"])?.forEach(([k, v]) => m.set(k, transformToObject(k, v)))
    return new GroupColumnSet(value["newField"], value["field"], m)
  } else if (clazz === PACKAGE + "parameter.QueryCacheParameter") {
    return new QueryCacheParameter(value["action"])
  } else if (clazz === PACKAGE + "AggregatedMeasure") {
    return new AggregatedMeasure(value["alias"], transformToObject("field", value["field"]), value["aggregationFunction"], value["distinct"], value["criteria"])
  } else if (clazz === PACKAGE + "ComparisonMeasureReferencePosition") {
    const m: Map<Field, any> = new Map
    value["referencePosition"] && Object.entries(value["referencePosition"])?.forEach(([k, v]) => m.set(transformToObject(k, JSON.parse(k)), v))
    return new ComparisonMeasureReferencePosition(
            value["alias"],
            value["comparisonMethod"],
            transformToObject("measure", value["measure"]),
            m.size == 0 ? undefined : m,
            value["columnSetKey"],
            value["elements"],
            value["period"],
            value["ancestors"]?.map((v: any) => transformToObject(undefined, v)),
            value["grandTotalAlongAncestors"])
  } else if (clazz === PACKAGE + "measure.ParametrizedMeasure") {
    return new ParametrizedMeasure(
            value["alias"],
            value["key"],
            value["parameters"])
  } else if (clazz === PACKAGE + "BinaryOperationMeasure") {
    return new BinaryOperationMeasure(
            value["alias"],
            value["operator"],
            transformToObject("leftOperand", value["leftOperand"]),
            transformToObject("rightOperand", value["rightOperand"]))
  } else if (clazz === PACKAGE + "ComparisonMeasureGrandTotal") {
    return new ComparisonMeasureGrandTotal(
            value["alias"],
            value["comparisonMethod"],
            transformToObject("measure", value["measure"]))
  } else if (clazz === PACKAGE + "PartialHierarchicalComparisonMeasure") {
    return new PartialHierarchicalComparisonMeasure(value["alias"], value["comparisonMethod"], transformToObject("measure", value["measure"]), value["axis"], value["grandTotalAlongAncestors"])
  } else if (clazz === PACKAGE + "ExpressionMeasure") {
    return new ExpressionMeasure(value["alias"], value["expression"])
  } else if (clazz === PACKAGE + "LongConstantMeasure") {
    return new LongConstantMeasure(value["value"])
  } else if (clazz === PACKAGE + "DoubleConstantMeasure") {
    return new DoubleConstantMeasure(value["value"])
  } else if (clazz === PACKAGE + "dto.CriteriaDto") {
    const c = value["children"]
    let children = undefined
    if (c) {
      children = []
      for (const cElement of c) {
        children.push(transformToObject(undefined, cElement))
      }
    }
    return new Criteria(
            transformToObject("field", value["field"]),
            transformToObject("fieldOther", value["fieldOther"]),
            transformToObject("measure", value["measure"]),
            transformToObject("condition", value["condition"]),
            value["conditionType"],
            children)
  } else if (clazz === PACKAGE + "dto.LogicalConditionDto") {
    return new LogicalCondition(value["type"], transformToObject("one", value["one"]), transformToObject("two", value["two"]))
  } else if (clazz === PACKAGE + "dto.InConditionDto") {
    return new InCondition(value["values"])
  } else if (clazz === PACKAGE + "dto.ConstantConditionDto") {
    return new ConstantCondition(value["type"])
  } else if (clazz === PACKAGE + "dto.SingleValueConditionDto") {
    return new SingleValueCondition(value["type"], value["value"])
  } else if (clazz === PACKAGE + "BinaryOperationField") {
    return new BinaryOperationField(value["operator"], transformToObject("leftOperand", value["leftOperand"]), transformToObject("rightOperand", value["rightOperand"]), value["alias"])
  } else if (clazz === PACKAGE + "ConstantField") {
    return new ConstantField(value["value"])
  } else if (clazz === PACKAGE + "dto.Period$Month") {
    return new Month(transformToObject("month", value["month"]), transformToObject("year", value["year"]))
  } else if (clazz === PACKAGE + "dto.Period$Quarter") {
    return new Quarter(transformToObject("quarter", value["quarter"]), transformToObject("year", value["year"]))
  } else if (clazz === PACKAGE + "dto.Period$Semester") {
    return new Semester(transformToObject("semester", value["semester"]), transformToObject("year", value["year"]))
  } else if (clazz === PACKAGE + "dto.Period$Year") {
    return new Year(transformToObject("year", value["year"]))
  } else if (clazz === PACKAGE + "dto.SimpleOrderDto") {
    return new SimpleOrder(value["order"])
  } else if (clazz === PACKAGE + "dto.ExplicitOrderDto") {
    return new ExplicitOrder(value["explicit"])
  }
  return reviverFallback ? reviverFallback(key, value) : value
}

function isParameter(v: any): v is Parameter {
  return typeof v === "object" && 'key' in v
}

export function squashQLReviver(key: string, value: any, reviverFallback?: (key: string, value: any) => any) {
  if (key === "columnSets") {
    const m: Map<string, ColumnSet> = new Map
    Object.entries(value)?.forEach(([k, v]) => m.set(k, transformToObject(k, v)))
    return m
  } else if (key === "orders") {
    const m: Map<Field, Order> = new Map
    value && Object.entries(value)?.forEach(([k, v]) => m.set(transformToObject(k, JSON.parse(k)), transformToObject(undefined, v)))
    return m
  } else if (key === "parameters") {
    // Special case for this key because Query and ParametrizedMeasure have an attribute called parameters
    let m = undefined
    for (const [k, v] of Object.entries(value)) {
      const o = transformToObject(k, v)
      if (isParameter(o)) {
        if (m === undefined) {
          m = new Map
        }
        m.set(k, o)
      } else {
        if (m === undefined) {
          m = {}
        }
        m[k] = o
      }
    }
    return m
  }

  if (typeof value === "object") {
    return transformToObject(key, value, reviverFallback)
  } else {
    return reviverFallback ? reviverFallback(key, value) : value
  }
}

export function deserialize(value: string): any {
  return JSON.parse(value, (k, v) => squashQLReviver(k, v))
}
