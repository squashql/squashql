export interface Condition {
    type: ConditionType;
}
export declare enum ConditionType {
    EQ = "EQ"
}
declare class SingleValueConditionDto implements Condition {
    type: ConditionType;
    value: any;
    constructor(type: ConditionType, value: any);
}
export declare function eq(value: any): SingleValueConditionDto;
export {};
