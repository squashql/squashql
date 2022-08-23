"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ge = exports.gt = exports.le = exports.lt = exports.neq = exports.eq = exports.or = exports.and = void 0;
var ConditionType;
(function (ConditionType) {
    ConditionType["EQ"] = "EQ";
    ConditionType["NEQ"] = "NEQ";
    ConditionType["LT"] = "LT";
    ConditionType["LE"] = "LE";
    ConditionType["GT"] = "GT";
    ConditionType["GE"] = "GE";
    ConditionType["AND"] = "AND";
    ConditionType["OR"] = "OR";
})(ConditionType || (ConditionType = {}));
class SingleValueConditionDto {
    constructor(type, value) {
        this.type = type;
        this.value = value;
    }
}
class LogicalConditionDto {
    constructor(type, one, two) {
        this.type = type;
        this.one = one;
        this.two = two;
    }
}
function and(left, right) {
    return new LogicalConditionDto(ConditionType.AND, left, right);
}
exports.and = and;
function or(left, right) {
    return new LogicalConditionDto(ConditionType.OR, left, right);
}
exports.or = or;
function eq(value) {
    return new SingleValueConditionDto(ConditionType.EQ, value);
}
exports.eq = eq;
function neq(value) {
    return new SingleValueConditionDto(ConditionType.NEQ, value);
}
exports.neq = neq;
function lt(value) {
    return new SingleValueConditionDto(ConditionType.LT, value);
}
exports.lt = lt;
function le(value) {
    return new SingleValueConditionDto(ConditionType.LE, value);
}
exports.le = le;
function gt(value) {
    return new SingleValueConditionDto(ConditionType.GT, value);
}
exports.gt = gt;
function ge(value) {
    return new SingleValueConditionDto(ConditionType.GE, value);
}
exports.ge = ge;
