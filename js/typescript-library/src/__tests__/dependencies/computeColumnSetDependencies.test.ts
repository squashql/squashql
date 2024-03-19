import * as dependencies from "../../dependencies"
import {TableField} from "../../field"
import {GroupColumnSet, ColumnSet} from "../../columnset"

afterEach(() => {
  jest.restoreAllMocks()
})

describe('computeColumnSetDependencies', () => {

  it('should compute dependencies for GroupColumnSet', () => {
    const createdField = new TableField('mockTable.createdField')
    const mockField = new TableField('mockTable.mockField')
    const columnSet = new GroupColumnSet(createdField, mockField, new Map())
    const result = dependencies.computeColumnSetDependencies(columnSet)

    expect(result).toEqual(expect.arrayContaining([mockField]))
  })

  it('should throw an error for unknown ColumnSet type', () => {
    class UnknownGroupColumnSet implements ColumnSet {
      readonly class: string
      readonly key: string
    }

    const unknownGroupColumnSet = new UnknownGroupColumnSet()

    expect(() => dependencies.computeColumnSetDependencies(unknownGroupColumnSet)).toThrow(
            "ColumnSet with unknown type: class UnknownGroupColumnSet"
    )
  })

})
