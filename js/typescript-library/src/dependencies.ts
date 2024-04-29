import {
  AggregatedMeasure,
  BinaryOperationMeasure,
  ComparisonMeasureGrandTotal,
  ComparisonMeasureReferencePosition,
  DoubleConstantMeasure,
  ExpressionMeasure,
  LongConstantMeasure,
  Measure
} from "./measure"
import {Condition, ConstantCondition, InCondition, LogicalCondition, SingleValueCondition} from "./condition"
import Criteria from "./criteria"
import {AliasedField, BinaryOperationField, ConstantField, Field, FunctionField, TableField} from "./field"
import {ColumnSet, GroupColumnSet} from "./columnset"
import {Month, Period, Quarter, Semester, Year} from "./period"

export const computeFieldDependencies = (field: Field, resultArray: Field[] = []): Field[] => {
  switch (field.constructor) {
    case TableField:
      if ((field as TableField).fullName !== "*") {
        resultArray.push(field as TableField)
      }
      break
    case AliasedField:
      resultArray.push(field)
      break
    case FunctionField: {
      const operand = (field as FunctionField).operand
      operand && resultArray.push(operand)
      break
    }
    case BinaryOperationField:
      computeFieldDependencies((field as BinaryOperationField).leftOperand, resultArray)
      computeFieldDependencies((field as BinaryOperationField).rightOperand, resultArray)
      break
    case ConstantField:
      break
    default:
      throw new Error("Field with unknown type: " + field.constructor)
  }

  return resultArray
}

export const computeColumnSetDependencies = (columnSet: ColumnSet, resultArray: Field[] = []): Field[] => {
  switch (columnSet.constructor) {
    case GroupColumnSet:
      computeFieldDependencies((columnSet as GroupColumnSet)["field"], resultArray)
      break
    default:
      throw new Error("ColumnSet with unknown type: " + columnSet.constructor)
  }

  return resultArray
}

export const computePeriodDependencies = (period: Period, resultArray: Field[] = []): Field[] => {
  switch (period.constructor) {
    case Year:
      computeFieldDependencies((period as Year)["year"], resultArray)
      break
    case Semester:
      computeFieldDependencies((period as Semester)["semester"], resultArray)
      computeFieldDependencies((period as Semester)["year"], resultArray)
      break
    case Quarter:
      computeFieldDependencies((period as Quarter)["quarter"], resultArray)
      computeFieldDependencies((period as Quarter)["year"], resultArray)
      break
    case Month:
      computeFieldDependencies((period as Month)["month"], resultArray)
      computeFieldDependencies((period as Month)["year"], resultArray)
      break
    default:
      throw new Error("Period with unknown type: " + period.constructor)
  }

  return resultArray
}

export const computeMeasureDependencies = (measure: Measure, resultArray: Field[] = []): Field[] => {
  switch (measure.constructor) {
    case AggregatedMeasure:
      computeFieldDependencies((measure as AggregatedMeasure).field, resultArray)
      if ((measure as AggregatedMeasure).criteria) {
        computeCriteriaDependencies((measure as AggregatedMeasure).criteria, resultArray)
      }
      break
    case BinaryOperationMeasure:
      computeMeasureDependencies((measure as BinaryOperationMeasure).leftOperand, resultArray)
      computeMeasureDependencies((measure as BinaryOperationMeasure).rightOperand, resultArray)
      break
    case ComparisonMeasureReferencePosition:
      computeMeasureDependencies((measure as ComparisonMeasureReferencePosition)["measure"], resultArray)
      if ((measure as ComparisonMeasureReferencePosition)["period"]) {
        computePeriodDependencies((measure as ComparisonMeasureReferencePosition)["period"], resultArray)
      }
      if ((measure as ComparisonMeasureReferencePosition)["ancestors"]) {
        (measure as ComparisonMeasureReferencePosition)["ancestors"]
                .forEach((field) => computeFieldDependencies(field, resultArray))
      }
      break
    case ComparisonMeasureGrandTotal:
      computeMeasureDependencies((measure as ComparisonMeasureGrandTotal)["measure"], resultArray)
      break
    case DoubleConstantMeasure:
    case LongConstantMeasure:
    case ExpressionMeasure:
      break
    default:
      throw new Error("Measure with unknown type: " + measure.constructor)
  }

  return resultArray
}

export const computeConditionDependencies = (condition: Condition, resultArray: Field[] = []): Field[] => {
  switch (condition.constructor) {
    case LogicalCondition:
      computeConditionDependencies((condition as LogicalCondition)["one"], resultArray)
      computeConditionDependencies((condition as LogicalCondition)["two"], resultArray)
      break
    case SingleValueCondition:
    case InCondition:
    case ConstantCondition:
      break
    default:
      throw new Error("Condition with unknown type: " + condition.constructor)
  }

  return resultArray
}

export const computeCriteriaDependencies = (criteria: Criteria, resultArray: Field[] = []): Field[] => {
  if (criteria.field) {
    computeFieldDependencies(criteria.field, resultArray)
  }
  if (criteria.fieldOther) {
    computeFieldDependencies(criteria.fieldOther, resultArray)
  }

  if (criteria["measure"]) {
    computeMeasureDependencies(criteria["measure"], resultArray)
  }
  if (criteria["condition"]) {
    computeConditionDependencies(criteria["condition"], resultArray)
  }

  if (criteria.children) {
    criteria.children.forEach((childCriteria) => {
      computeCriteriaDependencies(childCriteria, resultArray)
    })
  }

  return resultArray
}
