import {TableField} from "../../field"
import Criteria from "../../criteria"
import * as dependencies from "../../dependencies"
import {AggregatedMeasure} from "../../measure"
import {_in, ConditionType} from "../../condition"

afterEach(() => {
  jest.restoreAllMocks()
})

describe('computeCriteriaDependencies', () => {

  const mockField1 = new TableField('mockField1')
  const mockField2 = new TableField('mockField2')
  const mockField3 = new TableField('mockField3')
  const mockField4 = new TableField('mockField4')
  const mockField5 = new TableField('mockField5')
  const mockCondition2 = _in([33, 44])
  const mockCondition4 = _in([22])

  it('should compute dependencies for Criteria with fields', () => {
    const criteria = new Criteria(mockField1, mockField2, undefined, undefined, undefined, [])
    const result = dependencies.computeCriteriaDependencies(criteria)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]))
  })

  it('should compute dependencies for Criteria with measure', () => {
    const mockMeasure = new AggregatedMeasure('alias3', mockField1, 'SUM')
    const criteria = new Criteria(undefined, undefined, mockMeasure, undefined, undefined, [])
    const result = dependencies.computeCriteriaDependencies(criteria)

    expect(result).toEqual(expect.arrayContaining([mockField1]))
  })

  it('should compute no dependencies for Criteria with conditions', () => {
    const criteria = new Criteria(undefined, undefined, undefined, mockCondition2, undefined, [])
    const result = dependencies.computeCriteriaDependencies(criteria)

    expect(result.length).toEqual(0)
  })

  it('should compute dependencies for Criteria with nested criteria', () => {
    const nestedCriteria1 = new Criteria(mockField1, undefined, undefined, undefined, undefined, [])
    const nestedCriteria2 = new Criteria(mockField2, undefined, undefined, undefined, undefined, [])
    const criteria = new Criteria(undefined, undefined, undefined, undefined, undefined, [nestedCriteria1, nestedCriteria2])
    const result = dependencies.computeCriteriaDependencies(criteria)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]))
  })

  it('should compute dependencies for Criteria with fields, measure, conditions, and nested criteria', () => {
    const mockMeasure = new AggregatedMeasure('alias3', mockField3, 'SUM')
    const nestedCriteria1 = new Criteria(mockField4, undefined, undefined, undefined, undefined, [])
    const nestedCriteria2 = new Criteria(mockField5, undefined, undefined, undefined, undefined, [])

    const criteria = new Criteria(mockField1, mockField2, mockMeasure, mockCondition4, ConditionType.EQ, [nestedCriteria1, nestedCriteria2])
    const result = dependencies.computeCriteriaDependencies(criteria)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2, mockField3, mockField4, mockField5]))
  })

})
