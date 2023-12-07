import * as dependencies from "../../dependencies"
import {TableField} from "../../field"
import {BucketColumnSet, ColumnSet} from "../../columnsets"

afterEach(() => {
  jest.restoreAllMocks()
})

describe('computeColumnSetDependencies', () => {

  it('should compute dependencies for BucketColumnSet', () => {
    const createdField = new TableField('mockTable.createdField')
    const mockField = new TableField('mockTable.mockField')
    const columnSet = new BucketColumnSet(createdField, mockField, new Map())
    const result = dependencies.computeColumnSetDependencies(columnSet)

    expect(result).toEqual(expect.arrayContaining([mockField]))
  })

  it('should throw an error for unknown ColumnSet type', () => {
    class UnknownBucketColumnSet implements ColumnSet {
      readonly class: string
      readonly key: string
    }

    const unknownBucketColumnSet = new UnknownBucketColumnSet()

    expect(() => dependencies.computeColumnSetDependencies(unknownBucketColumnSet)).toThrow(
            "ColumnSet with unknown type: class UnknownBucketColumnSet"
    )
  })

})
