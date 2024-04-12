import {buildQuery} from "../generate-json-queryDto"
import {deserialize} from "../util"
import {Query} from "../query"
import {AliasedField, TableField} from "../field"
import {
  Axis,
  BinaryOperationMeasure,
  BinaryOperator,
  comparisonMeasureWithGrandTotal,
  comparisonMeasureWithGrandTotalAlongAncestors,
  comparisonMeasureWithParent,
  comparisonMeasureWithPeriod,
  ComparisonMethod,
  ExpressionMeasure,
  ParametrizedMeasure,
  PartialHierarchicalComparisonMeasure,
  sum,
  sumIf
} from "../measure"
import {any, criterion, eq} from "../condition"
import {Month} from "../period"

function checkQuery(expected: Query, actual: Query) {
  expect(actual.columns).toEqual(expected.columns)
  expect(actual.rollupColumns).toEqual(expected.rollupColumns)
  expect(actual.columnSets).toEqual(expected.columnSets)

  if (actual.parameters.size > 0) {
    expect(actual.parameters).toEqual(expected.parameters)
  } else {
    expect(expected.parameters).toBeUndefined()
  }
  expect(actual.measures).toEqual(expected.measures)
  expect(actual.table.joins).toEqual(expected.table.joins)
  expect(actual.table.name).toEqual(expected.table.name)
  if (expected.table.subQuery !== undefined) {
    checkQuery(actual.table.subQuery, expected.table.subQuery)
  }

  expect(actual.virtualTableDtos).toEqual(expected.virtualTableDtos)
  expect(actual.whereCriteriaDto).toEqual(expected.whereCriteriaDto)
  expect(actual.havingCriteriaDto).toEqual(expected.havingCriteriaDto)
  expect(actual.orders).toEqual(expected.orders)
  expect(actual.limit).toEqual(expected.limit)
  expect(actual.minify).toEqual(expected.minify)
}

// Fields
const a = new TableField("table.a")
const b = new TableField("table.b")
const c = new AliasedField("c")
const year = new TableField("table.year")
const month = new TableField("table.month")
const date = new TableField("table.date")

// Measures
const sumA = sum("sum_a", a)
const criteria = any([criterion(b, eq("b")), criterion(b, eq("bb"))])
const sumIfA = sumIf("sumIfA", a.divide(b.plus(c)), criterion(b, eq("bbb")))
const sumIfB = sumIf("sumIfB", b, criteria)
const expr = new ExpressionMeasure("expr", "my sql")
const bom = new BinaryOperationMeasure("binary measure", BinaryOperator.PLUS, sumA, expr)
const growth = comparisonMeasureWithPeriod("growth", ComparisonMethod.DIVIDE, sumA, new Map([
  [year, "y-1"],
  [month, "m"]
]), new Month(month, year))
const parent = comparisonMeasureWithParent("parent", ComparisonMethod.DIVIDE, sumA, [year, month])
const grandTotalAlongAncestors = comparisonMeasureWithGrandTotalAlongAncestors("grandTotalAlongAncestors", ComparisonMethod.DIVIDE, sumA, [year, month])
const grandTotal = comparisonMeasureWithGrandTotal("grandTotal", ComparisonMethod.DIVIDE, sumA)
const var95 = new ParametrizedMeasure("var measure", "VAR", {
  "value": a,
  "date": date,
  "quantile": 0.95
})
const incrVar95 = new ParametrizedMeasure("incr var measure", "INCREMENTAL_VAR", {
  "value": a,
  "date": date,
  "quantile": 0.95,
  "ancestors": [a, b, c],
})
const percentOfParentAlongAncestors = new PartialHierarchicalComparisonMeasure("pop along ancestors", ComparisonMethod.DIVIDE, sumA, Axis.ROW, false)
const compareWithGrandTotalAlongAncestors = new PartialHierarchicalComparisonMeasure("gt along ancestors", ComparisonMethod.DIVIDE, sumA, Axis.ROW, false)


describe('serialization', () => {

  test('serialize query', () => {
    const data = buildQuery()
    const obj = deserialize(JSON.stringify(data)) as Query
    checkQuery(data, obj)
  })

  test('serialize criteria', () => {
    const obj = deserialize(JSON.stringify(criteria))
    expect(criteria).toEqual(obj)
  })

  test('serialize sumIf simple', () => {
    const obj = deserialize(JSON.stringify(sumIfB))
    expect(sumIfB).toEqual(obj)
  })

  test('serialize sumIf complex', () => {
    const obj = deserialize(JSON.stringify(sumIfA))
    expect(sumIfA).toEqual(obj)
  })

  test('serialize comparisonMeasureWithPeriod', () => {
    const obj = deserialize(JSON.stringify(growth))
    expect(growth).toEqual(obj)
  })

  test('serialize comparisonMeasureWithParent', () => {
    const obj = deserialize(JSON.stringify(parent))
    expect(parent).toEqual(obj)
  })

  test('serialize comparisonMeasureWithGrandTotalAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(grandTotalAlongAncestors))
    expect(grandTotalAlongAncestors).toEqual(obj)
  })

  test('serialize comparisonMeasureWithGrandTotal', () => {
    const obj = deserialize(JSON.stringify(grandTotal))
    expect(grandTotal).toEqual(obj)
  })

  test('serialize BinaryOperationMeasure', () => {
    const obj = deserialize(JSON.stringify(bom))
    expect(bom).toEqual(obj)
  })

  test('serialize percentOfParentAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(percentOfParentAlongAncestors))
    expect(percentOfParentAlongAncestors).toEqual(obj)
  })

  test('serialize compareWithGrandTotalAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(compareWithGrandTotalAlongAncestors))
    expect(compareWithGrandTotalAlongAncestors).toEqual(obj)
  })

  test('serialize var', () => {
    const obj = deserialize(JSON.stringify(var95))
    expect(var95).toEqual(obj)
  })

  test('serialize incr var', () => {
    const obj = deserialize(JSON.stringify(incrVar95))
    expect(incrVar95).toEqual(obj)
  })
})
