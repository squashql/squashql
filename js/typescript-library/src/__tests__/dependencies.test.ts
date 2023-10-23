import {BinaryOperationField, ConstantField, TableField} from "../field";
import * as dependencies from "../dependencies";
import {BucketColumnSet, Month, Quarter, Semester, Year} from "../columnsets";
import {BinaryOperator, ColumnSet, ColumnSetKey, Field, Measure, Period} from "../types";
import {
  AggregatedMeasure,
  BinaryOperationMeasure,
  ComparisonMeasureReferencePosition,
  ComparisonMethod,
  DoubleConstantMeasure,
  ExpressionMeasure,
  LongConstantMeasure
} from "../measure";
import Criteria from "../criteria";

afterEach(() => {
  jest.restoreAllMocks();
});

describe('computeFieldDependencies', () => {

  it('should return the correct dependencies for a TableField', () => {
    const field = new TableField('tableName.fieldName');
    const result = dependencies.computeFieldDependencies(field);
    expect(result).toEqual(expect.arrayContaining([field]));
  });

  it('should return dependencies for BinaryOperationField with two TableFields', () => {
    const leftOperand = new TableField('tableName1.fieldName1');
    const rightOperand = new TableField('tableName2.fieldName2');
    const field = new BinaryOperationField(BinaryOperator.PLUS, leftOperand, rightOperand);
    const result = dependencies.computeFieldDependencies(field);
    expect(result).toEqual(expect.arrayContaining([leftOperand, rightOperand]));
  });

  it('should return dependencies for nested BinaryOperationFields', () => {
    const leftOperand1 = new TableField('tableName1.fieldName1');
    const rightOperand1 = new TableField('tableName2.fieldName2');
    const leftOperand2 = new TableField('tableName3.fieldName3');
    const binaryOp1 = new BinaryOperationField(BinaryOperator.PLUS, leftOperand1, rightOperand1);
    const field = new BinaryOperationField(BinaryOperator.MINUS, binaryOp1, leftOperand2);
    const result = dependencies.computeFieldDependencies(field);
    expect(result).toEqual(expect.arrayContaining([leftOperand1, rightOperand1, leftOperand2]));
  });

  it('should return an empty array for ConstantField', () => {
    const field = new ConstantField(5);
    const result = dependencies.computeFieldDependencies(field);
    expect(result).toEqual([]);
  });

  it('should return an empty array for TableField with *', () => {
    const field = new TableField('*');
    const result = dependencies.computeFieldDependencies(field);
    expect(result).toEqual([]);
  });

  it('should throw an error for unknown field type', () => {
    class UnknownField implements Field {
      readonly class: string;

      divide(other: Field): Field {
        return undefined;
      }

      minus(other: Field): Field {
        return undefined;
      }

      multiply(other: Field): Field {
        return undefined;
      }

      plus(other: Field): Field {
        return undefined;
      }
    }

    const unknownField = new UnknownField();
    expect(() => dependencies.computeFieldDependencies(unknownField)).toThrow('Field with unknown type: class UnknownField');
  });

});

describe('computeColumnSetDependencies', () => {

  it('should compute dependencies for BucketColumnSet', () => {
    const createdField = new TableField('mockTable.createdField');
    const mockField = new TableField('mockTable.mockField');
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField) {
        array.push(mockField);
      }
      return array;
    });

    const columnSet = new BucketColumnSet(createdField, mockField, new Map());
    const result = dependencies.computeColumnSetDependencies(columnSet);

    expect(result).toEqual(expect.arrayContaining([mockField]));
  });

  it('should throw an error for unknown ColumnSet type', () => {
    class UnknownBucketColumnSet implements ColumnSet {
      readonly class: string;
      readonly key: string;
    }

    const unknownBucketColumnSet = new UnknownBucketColumnSet();

    expect(() => dependencies.computeColumnSetDependencies(unknownBucketColumnSet)).toThrow(
            "ColumnSet with unknown type: class UnknownBucketColumnSet"
    );
  });

});

describe('computePeriodDependencies', () => {

  const mockYear = new TableField('mockYear');
  const mockSemester = new TableField('mockSemester');
  const mockQuarter = new TableField('mockQuarter');
  const mockMonth = new TableField('mockMonth');

  it('should compute dependencies for Year', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Year(mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockYear]));
  });

  it('should compute dependencies for Semester', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockSemester) {
        array.push(mockSemester);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Semester(mockSemester, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockSemester, mockYear]));
  });

  it('should compute dependencies for Quarter', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockQuarter) {
        array.push(mockQuarter);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Quarter(mockQuarter, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockQuarter, mockYear]));
  });

  it('should compute dependencies for Month', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockMonth) {
        array.push(mockMonth);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Month(mockMonth, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockMonth, mockYear]));
  });

  it('should throw an error for unknown Period type', () => {
    class UnknownPeriod implements Period {
      readonly class: string;
    }

    const unknownPeriod = new UnknownPeriod();

    expect(() => dependencies.computePeriodDependencies(unknownPeriod)).toThrow(
            "Period with unknown type: class UnknownPeriod"
    );
  });

});

describe('computeMeasureDependencies', () => {

  const mockField1 = new TableField('mockField1');
  const mockField2 = new TableField('mockField2');
  const mockField3 = new TableField('mockField3');
  const mockField4 = new TableField('mockField3');
  const mockMeasure1 = new AggregatedMeasure('alias1', mockField1, 'SUM');
  const mockMeasure2 = new AggregatedMeasure('alias2', mockField2, 'SUM');

  it('should compute dependencies for AggregatedMeasure', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1) {
        array.push(field as TableField);
      }
      return array;
    });

    const measure = new AggregatedMeasure('alias', mockField1, 'SUM');
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result).toEqual(expect.arrayContaining([mockField1]));
  });

  it('should compute dependencies for AggregatedMeasure with criteria', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1) {
        array.push(mockField1);
      }
      return array;
    });

    const mockCriteria = new Criteria(mockField2, null, mockMeasure2, null, null, []);
    const computeCriteriaDependenciesSpy = jest.spyOn(dependencies, 'computeCriteriaDependencies');
    computeCriteriaDependenciesSpy.mockImplementation((criteria, array) => {
      if (criteria == mockCriteria) {
        array.push(mockField2);
      }
      return array;
    });

    const measure = new AggregatedMeasure('alias', mockField1, 'SUM', false, mockCriteria);
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute dependencies for BinaryOperationMeasure', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');

    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1 || field === mockField2) {
        array.push(field as TableField);
      }
      return array;
    });

    const measure = new BinaryOperationMeasure('alias', BinaryOperator.PLUS, mockMeasure1, mockMeasure2);
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]));
  });

  it('should compute dependencies for ComparisonMeasureReferencePosition', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1) {
        array.push(field as TableField);
      }
      return array;
    });

    const measure = new ComparisonMeasureReferencePosition('alias', ComparisonMethod.ABSOLUTE_DIFFERENCE, mockMeasure1, new Map([[mockField1, 'value1']]));
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result).toEqual(expect.arrayContaining([mockField1]));
  });

  it('should compute dependencies for ComparisonMeasureReferencePosition with all optional arguments', () => {
    const mockPeriod = new Year(mockField2);
    const computePeriodDependenciesSpy = jest.spyOn(dependencies, 'computePeriodDependencies');
    computePeriodDependenciesSpy.mockImplementation((period, array) => {
      if (period === mockPeriod) {
        array.push(mockField2);
      }
      return array;
    });

    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField1 || field === mockField3 || field === mockField4) {
        array.push(field as TableField);
      }
      return array;
    });

    const referencePosition = new Map([[mockField1, 'value1']]);
    const ancestors = [mockField3, mockField4];
    const measure = new ComparisonMeasureReferencePosition(
            'alias',
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            mockMeasure1,
            referencePosition,
            ColumnSetKey.BUCKET,
            mockPeriod,
            ancestors
    );
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2, mockField3, mockField4]));
  });

  it('should compute dependencies for DoubleConstantMeasure', () => {
    const measure = new DoubleConstantMeasure(100.5);
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result.length).toEqual(0);
  });

  it('should compute dependencies for LongConstantMeasure', () => {
    const measure = new LongConstantMeasure(100);
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result.length).toEqual(0);
  });

  it('should compute dependencies for ExpressionMeasure', () => {
    const measure = new ExpressionMeasure("test", "2 * 2 * 2");
    const result = dependencies.computeMeasureDependencies(measure);

    expect(result.length).toEqual(0);
  });

  it('should throw an error for unknown Measure type', () => {
    class UnknownMeasure implements Measure {
      readonly alias: string;
      readonly class: string;
    }

    const unknownMeasure = new UnknownMeasure();

    expect(() => dependencies.computeMeasureDependencies(unknownMeasure)).toThrow(
            "Measure with unknown type: class UnknownMeasure"
    );
  });

});

