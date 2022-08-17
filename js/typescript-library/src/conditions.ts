export interface Condition {
    type: ConditionType;
}

export enum ConditionType {
    EQ = "EQ",
}

class SingleValueConditionDto implements Condition {
    type: ConditionType
    value: any

    constructor(type: ConditionType, value: any) {
        this.type = type
        this.value = value
    }
}

export function eq(value: any) {
    return new SingleValueConditionDto(ConditionType.EQ, value)
}