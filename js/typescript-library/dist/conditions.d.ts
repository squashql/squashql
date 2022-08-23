export interface Condition {
    readonly type: ConditionType;
}
declare enum ConditionType {
    EQ = "EQ",
    NEQ = "NEQ",
    LT = "LT",
    LE = "LE",
    GT = "GT",
    GE = "GE",
    AND = "AND",
    OR = "OR"
}
declare class SingleValueConditionDto implements Condition {
    readonly type: ConditionType;
    private value;
    constructor(type: ConditionType, value: any);
}
declare class LogicalConditionDto implements Condition {
    readonly type: ConditionType;
    private one;
    private two;
    constructor(type: ConditionType, one: Condition, two: Condition);
}
export declare function and(left: Condition, right: Condition): LogicalConditionDto;
export declare function or(left: Condition, right: Condition): LogicalConditionDto;
export declare function eq(value: any): SingleValueConditionDto;
export declare function neq(value: any): SingleValueConditionDto;
export declare function lt(value: any): SingleValueConditionDto;
export declare function le(value: any): SingleValueConditionDto;
export declare function gt(value: any): SingleValueConditionDto;
export declare function ge(value: any): SingleValueConditionDto;
export {};
