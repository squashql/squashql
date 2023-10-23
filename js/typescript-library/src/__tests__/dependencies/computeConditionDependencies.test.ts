import * as dependencies from "../../dependencies";
import {TableField} from "../../field";
import {ConstantCondition, InCondition, LogicalCondition, SingleValueCondition} from "../../conditions";
import {Condition, ConditionType} from "../../types";

afterEach(() => {
  jest.restoreAllMocks();
});

describe('computeConditionDependencies', () => {

  const mockField1 = new TableField('mockField1');
  const mockField2 = new TableField('mockField2');

  it('should compute dependencies for SingleValueCondition', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1) {
        array.push(field as TableField);
      }
      return array;
    });

    const condition = new SingleValueCondition(ConditionType.EQ, mockField1);
    const result = dependencies.computeConditionDependencies(condition);

    expect(result).toEqual(expect.arrayContaining([mockField1]));
  });

  it('should compute dependencies for InCondition', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1) {
        array.push(field as TableField);
      }
      return array;
    });

    const condition = new InCondition([mockField1]);
    const result = dependencies.computeConditionDependencies(condition);

    expect(result).toEqual(expect.arrayContaining([mockField1]));
  });

  it('should compute dependencies for LogicalCondition', () => {
    const spy = jest.spyOn(dependencies, 'computeFieldDependencies');
    spy.mockImplementation((field, array) => {
      if (field === mockField1 || field === mockField2) {
        array.push(field as TableField);
      }
      return array;
    });

    const conditionOne = new SingleValueCondition(ConditionType.EQ, mockField1);
    const conditionTwo = new SingleValueCondition(ConditionType.EQ, mockField2);
    const condition = new LogicalCondition(ConditionType.AND, conditionOne, conditionTwo);
    const result = dependencies.computeConditionDependencies(condition);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute no dependencies for ConstantCondition', () => {
    const condition = new ConstantCondition(ConditionType.NULL);
    const result = dependencies.computeConditionDependencies(condition);

    expect(result.length).toEqual(0);
  });

  it('should throw an error for unknown Condition type', () => {
    class UnknownCondition implements Condition {
      readonly class: string;
      readonly type: ConditionType;
    }

    const unknownCondition = new UnknownCondition();

    expect(() => dependencies.computeConditionDependencies(unknownCondition)).toThrow(
            "Condition with unknown type: class UnknownCondition"
    );
  });

});
