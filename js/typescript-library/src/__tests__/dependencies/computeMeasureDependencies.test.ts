import {TableField} from "../../field"
import {
    AggregatedMeasure,
    BinaryOperationMeasure,
    BinaryOperator,
    ComparisonMeasureReferencePosition, ComparisonMethod, DoubleConstantMeasure, ExpressionMeasure, LongConstantMeasure
} from "../../measure"
import {Criteria, Measure, Year} from "../../index"
import * as dependencies from "../../dependencies"
import {ColumnSetKey} from "../../columnsets"

afterEach(() => {
  jest.restoreAllMocks()
})

describe('computeMeasureDependencies', () => {

  const mockField1 = new TableField('mockField1')
  const mockField2 = new TableField('mockField2')
  const mockField3 = new TableField('mockField3')
  const mockField4 = new TableField('mockField3')
  const mockMeasure1 = new AggregatedMeasure('alias1', mockField1, 'SUM')
  const mockMeasure2 = new AggregatedMeasure('alias2', mockField2, 'SUM')

  it('should compute dependencies for AggregatedMeasure', () => {
    const measure = new AggregatedMeasure('alias', mockField1, 'SUM')
    const array = []
    const result = dependencies.computeMeasureDependencies(measure, array)

    expect(result).toEqual(expect.arrayContaining([mockField1]))
  })

  it('should compute dependencies for AggregatedMeasure with criteria', () => {
    const mockCriteria = new Criteria(mockField2, null, mockMeasure2, null, null, [])
    const measure = new AggregatedMeasure('alias', mockField1, 'SUM', false, mockCriteria)
    const array = []
    const result = dependencies.computeMeasureDependencies(measure, array)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]))
  })

  it('should compute dependencies for BinaryOperationMeasure', () => {
    const array = []
    const measure = new BinaryOperationMeasure('alias', BinaryOperator.PLUS, mockMeasure1, mockMeasure2)
    const result = dependencies.computeMeasureDependencies(measure, array)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2]))
  })

  it('should compute dependencies for ComparisonMeasureReferencePosition', () => {
    const array = []
    const measure = new ComparisonMeasureReferencePosition('alias', ComparisonMethod.ABSOLUTE_DIFFERENCE, mockMeasure1, new Map([[mockField1, 'value1']]))
    const result = dependencies.computeMeasureDependencies(measure, array)

    expect(result).toEqual(expect.arrayContaining([mockField1]))
  })

  it('should compute dependencies for ComparisonMeasureReferencePosition with all optional arguments', () => {
    const mockPeriod = new Year(mockField2)
    const referencePosition = new Map([[mockField1, 'value1']])
    const ancestors = [mockField3, mockField4]
    const measure = new ComparisonMeasureReferencePosition(
            'alias',
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            mockMeasure1,
            referencePosition,
            ColumnSetKey.BUCKET,
            mockPeriod,
            ancestors
    )
    const array = []
    const result = dependencies.computeMeasureDependencies(measure, array)

    expect(result).toEqual(expect.arrayContaining([mockField1, mockField2, mockField3, mockField4]))
  })

  it('should compute dependencies for DoubleConstantMeasure', () => {
    const measure = new DoubleConstantMeasure(100.5)
    const result = dependencies.computeMeasureDependencies(measure)

    expect(result.length).toEqual(0)
  })

  it('should compute dependencies for LongConstantMeasure', () => {
    const measure = new LongConstantMeasure(100)
    const result = dependencies.computeMeasureDependencies(measure)

    expect(result.length).toEqual(0)
  })

  it('should compute dependencies for ExpressionMeasure', () => {
    const measure = new ExpressionMeasure("test", "2 * 2 * 2")
    const result = dependencies.computeMeasureDependencies(measure)

    expect(result.length).toEqual(0)
  })

  it('should throw an error for unknown Measure type', () => {
    class UnknownMeasure implements Measure {
      readonly alias: string
      readonly class: string
    }

    const unknownMeasure = new UnknownMeasure()

    expect(() => dependencies.computeMeasureDependencies(unknownMeasure)).toThrow(
            "Measure with unknown type: class UnknownMeasure"
    )
  })

})
