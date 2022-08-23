import { Condition } from "./conditions";
export interface Measure {
    readonly class: string;
    expression?: string;
}
export declare class AggregatedMeasure implements Measure {
    class: string;
    field: string;
    aggregationFunction: string;
    alias: string;
    expression?: string;
    conditionField?: string;
    condition?: Condition;
    constructor(field: string, aggregationFunction: string, alias: string, conditionField?: string, condition?: Condition);
    toJSON(): {
        "@class": string;
        field: string;
        aggregation_function: string;
        alias: string;
        expression: string;
        condition_field: string;
        condition_dto: Condition;
    };
}
export declare class ExpressionMeasure implements Measure {
    private alias;
    private sqlExpression;
    class: string;
    constructor(alias: string, sqlExpression: string);
    toJSON(): {
        "@class": string;
        alias: string;
        expression: string;
    };
}
export declare class BinaryOperationMeasure implements Measure {
    class: string;
    alias?: string;
    expression?: string;
    operator: BinaryOperator;
    leftOperand: Measure;
    rightOperand: Measure;
    constructor(alias: string, operator: BinaryOperator, leftOperand: Measure, rightOperand: Measure);
    toJSON(): {
        "@class": string;
        alias: string;
        operator: BinaryOperator;
        left_operand: Measure;
        right_operand: Measure;
    };
}
export declare enum BinaryOperator {
    PLUS = "PLUS",
    MINUS = "MINUS",
    MULTIPLY = "MULTIPLY",
    DIVIDE = "DIVIDE"
}
