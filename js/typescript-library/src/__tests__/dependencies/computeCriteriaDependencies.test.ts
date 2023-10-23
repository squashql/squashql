import {TableField} from "../../field";
import {InCondition} from "../../conditions";
import Criteria from "../../criteria";
import * as dependencies from "../../dependencies";
import {AggregatedMeasure} from "../../measure";
import {ConditionType} from "../../types";

afterEach(() => {
  jest.restoreAllMocks();
});

describe('computeCriteriaDependencies', () => {

  const mockField1 = new TableField('mockField1');
  const mockField2 = new TableField('mockField2');
  const mockField3 = new TableField('mockField3');
  const mockField4 = new TableField('mockField4');
  const mockField5 = new TableField('mockField5');
  const mockField6 = new TableField('mockField6');
  const mockCondition2 = new InCondition([mockField1, mockField2]);
  const mockCondition4 = new InCondition([mockField4]);

  it('should compute dependencies for Criteria with fields', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1 || field === mockField2) {
        array.push(field as TableField);
      }
      return array;
    });

    const criteria = new Criteria(mockField1, mockField2, undefined, undefined, undefined, []);
    const result = dependencies.computeCriteriaDependencies(criteria);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute dependencies for Criteria with measure', () => {
    const mockMeasure = new AggregatedMeasure('alias3', mockField1, 'SUM');

    const computeMeasureDependenciesSpy = jest.spyOn(dependencies, 'computeMeasureDependencies');
    computeMeasureDependenciesSpy.mockImplementation((measure, array) => {
      if (measure === mockMeasure) {
        array.push(mockField1);
      }
      return array;
    });

    const criteria = new Criteria(undefined, undefined, mockMeasure, undefined, undefined, []);
    const result = dependencies.computeCriteriaDependencies(criteria);

    expect(result).toEqual(expect.arrayContaining([mockField1]));
  });

  it('should compute dependencies for Criteria with conditions', () => {
    const computeConditionDependenciesSpy = jest.spyOn(dependencies, 'computeConditionDependencies');
    computeConditionDependenciesSpy.mockImplementation((condition, array) => {
      if (condition === mockCondition2) {
        array.push(mockField1, mockField2);
      }
      return array;
    });

    const criteria = new Criteria(undefined, undefined, undefined, mockCondition2, undefined, []);
    const result = dependencies.computeCriteriaDependencies(criteria);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute dependencies for Criteria with nested criteria', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1 || field === mockField2) {
        array.push(field as TableField);
      }
      return array;
    });

    const nestedCriteria1 = new Criteria(mockField1, undefined, undefined, undefined, undefined, []);
    const nestedCriteria2 = new Criteria(mockField2, undefined, undefined, undefined, undefined, []);
    const criteria = new Criteria(undefined, undefined, undefined, undefined, undefined, [nestedCriteria1, nestedCriteria2]);
    const result = dependencies.computeCriteriaDependencies(criteria);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute dependencies for Criteria with fields, measure, conditions, and nested criteria', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if ([mockField1, mockField2, mockField5, mockField6]
              .find((mockField) => mockField === field) !== undefined) {
        array.push(field as TableField);
      }
      return array;
    });

    const mockMeasure = new AggregatedMeasure('alias3', mockField3, 'SUM');
    const computeMeasureDependenciesSpy = jest.spyOn(dependencies, 'computeMeasureDependencies');
    computeMeasureDependenciesSpy.mockImplementation((measure, array) => {
      if (measure === mockMeasure) {
        array.push(mockField3);
      }
      return array;
    });

    const computeConditionDependenciesSpy = jest.spyOn(dependencies, 'computeConditionDependencies');
    computeConditionDependenciesSpy.mockImplementation((condition, array) => {
      if (condition === mockCondition4) {
        array.push(mockField4);
      }
      return array;
    });

    const nestedCriteria1 = new Criteria(mockField5, undefined, undefined, undefined, undefined, []);
    const nestedCriteria2 = new Criteria(mockField6, undefined, undefined, undefined, undefined, []);

    const criteria = new Criteria(mockField1, mockField2, mockMeasure, mockCondition4, ConditionType.EQ, [nestedCriteria1, nestedCriteria2]);
    const result = dependencies.computeCriteriaDependencies(criteria);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2, mockField3, mockField4, mockField5, mockField6]));
  });

});
