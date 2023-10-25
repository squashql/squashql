import * as dependencies from "../../dependencies"
import {
  Condition,
  ConditionType,
  ConstantCondition,
  InCondition,
  LogicalCondition,
  SingleValueCondition
} from "../../conditions"

afterEach(() => {
  jest.restoreAllMocks()
})

describe('computeConditionDependencies', () => {

  it('should compute no dependencies for SingleValueCondition', () => {
    const condition = new SingleValueCondition(ConditionType.EQ, 77)
    const result = dependencies.computeConditionDependencies(condition)

    expect(result.length).toEqual(0)
  })

  it('should compute no dependencies for InCondition', () => {
    const condition = new InCondition([88])
    const result = dependencies.computeConditionDependencies(condition)

    expect(result.length).toEqual(0)
  })

  it('should compute no dependencies for LogicalCondition', () => {
    const conditionOne = new SingleValueCondition(ConditionType.EQ, 77)
    const conditionTwo = new SingleValueCondition(ConditionType.EQ, 88)
    const condition = new LogicalCondition(ConditionType.AND, conditionOne, conditionTwo)
    const result = dependencies.computeConditionDependencies(condition)

    expect(result.length).toEqual(0)
  })

  it('should compute no dependencies for ConstantCondition', () => {
    const condition = new ConstantCondition(ConditionType.NULL)
    const result = dependencies.computeConditionDependencies(condition)

    expect(result.length).toEqual(0)
  })

  it('should throw an error for unknown Condition type', () => {
    class UnknownCondition implements Condition {
      readonly class: string
      readonly type: ConditionType
    }

    const unknownCondition = new UnknownCondition()

    expect(() => dependencies.computeConditionDependencies(unknownCondition)).toThrow(
            "Condition with unknown type: class UnknownCondition"
    )
  })

})
