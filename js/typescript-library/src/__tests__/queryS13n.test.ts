import {buildQuery} from "../generate-json-queryDto"
import {deserialize} from "../util"
import {Query} from "../query"

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
  // expect(actual.table).toEqual(expected.table)
  expect(actual.table.joins).toEqual(expected.table.joins)
  expect(actual.table.name).toEqual(expected.table.name)
  if (expected.table.subQuery !== undefined) {
    checkQuery(actual.table.subQuery, expected.table.subQuery)
  }

  // if (expected.virtualTables && expected.virtualTables.length > 0) {
  //   expect(actual.virtualTables).toEqual(expected.virtualTables)
  // } else {
  //   // expect(actual.virtualTables).toBeUndefined()
  //   expect(actual.virtualTables.length).toEqual(0)
  // }

  expect(actual.whereCriteria).toEqual(expected.whereCriteria)
  expect(actual.havingCriteria).toEqual(expected.havingCriteria)
  expect(actual.orders).toEqual(expected.orders)
  expect(actual.limit).toEqual(expected.limit)
  expect(actual.minify).toEqual(expected.minify)
}

describe('serialization', () => {

  const data = buildQuery()
  const obj = deserialize(JSON.stringify(data)) as Query

  test('serialize query', () => {
    checkQuery(data, obj)
  })
})
