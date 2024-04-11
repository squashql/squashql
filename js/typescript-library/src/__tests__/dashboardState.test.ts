import {buildQuery} from "../generate-json-queryDto"
import {deserialize} from "../util"
import {Query} from "../query"

describe('serialization', () => {
  const data = buildQuery()
  // console.log(data)
  const s = JSON.stringify(data)
  const obj = deserialize(s) as Query
  test('serialize criteria', () => {
    // const json = serialize_(criteria)
    // const obj = deserialize_(json)
    expect(data.columns).toEqual(obj.columns)
    expect(data.rollupColumns).toEqual(obj.rollupColumns)
    expect(data.columnSets).toEqual(obj.columnSets)
    expect(data.parameters).toEqual(obj.parameters)
    expect(data.measures).toEqual(obj.measures)
    // expect(data.table).toEqual(obj.table)
    // expect(data.virtualTables).toEqual(obj.virtualTables)
    // expect(data.whereCriteria).toEqual(obj.whereCriteria)
    // expect(data.havingCriteriaDto).toEqual(obj.havingCriteriaDto)
    // expect(data.orders).toEqual(obj.orders)
    // expect(data.limit).toEqual(obj.limit)
    // expect(data.minify).toEqual(obj.minify)
  })

  test('serialize sumIf simple', () => {
    // const json = serialize_(sumIfB)
    // const obj = deserialize_(json)
    // expect(sumIfB).toEqual(obj)
  })
})
