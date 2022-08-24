export interface Condition {
    readonly class: string;
    readonly type: ConditionType;
}
declare enum ConditionType {
    EQ = "EQ",
    NEQ = "NEQ",
    LT = "LT",
    LE = "LE",
    GT = "GT",
    GE = "GE",
    IN = "IN",
    AND = "AND",
    OR = "OR"
}
export declare function and(left: Condition, right: Condition): Condition;
export declare function or(left: Condition, right: Condition): Condition;
export declare function _in(value: Array<any>): Condition;
export declare function eq(value: any): Condition;
export declare function neq(value: any): Condition;
export declare function lt(value: any): Condition;
export declare function le(value: any): Condition;
export declare function gt(value: any): Condition;
export declare function ge(value: any): Condition;
export {};
