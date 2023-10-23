import {BinaryOperationField, ConstantField, TableField} from "../../field";
import * as dependencies from "../../dependencies";
import {BinaryOperator, Field} from "../../types";

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
