import {buildQuery} from "../generate-json-queryDto"
import {deserialize, squashQLReviver} from "../util"
import {Query} from "../query"
import {AliasedField, Field, TableField} from "../field"
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

  test('deserialize query', () => {
    const data = buildQuery()
    const obj = deserialize(JSON.stringify(data)) as Query
    checkQuery(data, obj)
  })

  test('deserialize criteria', () => {
    const obj = deserialize(JSON.stringify(criteria))
    expect(criteria).toEqual(obj)
  })

  test('deserialize sumIf simple', () => {
    const obj = deserialize(JSON.stringify(sumIfB))
    expect(sumIfB).toEqual(obj)
  })

  test('deserialize sumIf complex', () => {
    const obj = deserialize(JSON.stringify(sumIfA))
    expect(sumIfA).toEqual(obj)
  })

  test('deserialize comparisonMeasureWithPeriod', () => {
    const obj = deserialize(JSON.stringify(growth))
    expect(growth).toEqual(obj)
  })

  test('deserialize comparisonMeasureWithParent', () => {
    const obj = deserialize(JSON.stringify(parent))
    expect(parent).toEqual(obj)
  })

  test('deserialize comparisonMeasureWithGrandTotalAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(grandTotalAlongAncestors))
    expect(grandTotalAlongAncestors).toEqual(obj)
  })

  test('deserialize comparisonMeasureWithGrandTotal', () => {
    const obj = deserialize(JSON.stringify(grandTotal))
    expect(grandTotal).toEqual(obj)
  })

  test('deserialize BinaryOperationMeasure', () => {
    const obj = deserialize(JSON.stringify(bom))
    expect(bom).toEqual(obj)
  })

  test('deserialize percentOfParentAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(percentOfParentAlongAncestors))
    expect(percentOfParentAlongAncestors).toEqual(obj)
  })

  test('deserialize compareWithGrandTotalAlongAncestors', () => {
    const obj = deserialize(JSON.stringify(compareWithGrandTotalAlongAncestors))
    expect(compareWithGrandTotalAlongAncestors).toEqual(obj)
  })

  test('deserialize var', () => {
    const obj = deserialize(JSON.stringify(var95))
    expect(var95).toEqual(obj)
  })

  test('deserialize incr var', () => {
    const obj = deserialize(JSON.stringify(incrVar95))
    expect(incrVar95).toEqual(obj)
  })

  test('deserialize with fallback', () => {
    // Implement a custom type.
    class UnknownField implements Field {
      readonly class: string = "custom.UnknownField"

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      divide(other: Field): Field {
        return undefined
      }

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      minus(other: Field): Field {
        return undefined
      }

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      multiply(other: Field): Field {
        return undefined
      }

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      plus(other: Field): Field {
        return undefined
      }

      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      as(alias: string): Field {
        return undefined
      }

      toJSON() {
        return {
          "@class": this.class,
          "field": "123"
        }
      }
    }

    const unknownField = new UnknownField()
    const data = {
      "a": a,
      "unknown": unknownField
    }
    const str = JSON.stringify(data)
    const obj = JSON.parse(str, (k, v) => squashQLReviver(k, v, (key, value) => {
      if (value["@class"] === "custom.UnknownField") {
        return new UnknownField()
      } else {
        return value
      }
    }))
    expect(data).toEqual(obj)
  })
})
