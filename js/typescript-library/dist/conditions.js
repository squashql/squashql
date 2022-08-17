"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.eq = exports.ConditionType = void 0;
var ConditionType;
(function (ConditionType) {
    ConditionType["EQ"] = "EQ";
})(ConditionType = exports.ConditionType || (exports.ConditionType = {}));
class SingleValueConditionDto {
    constructor(type, value) {
        this.type = type;
        this.value = value;
    }
}
function eq(value) {
    return new SingleValueConditionDto(ConditionType.EQ, value);
}
exports.eq = eq;
