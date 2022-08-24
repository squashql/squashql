"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ge = exports.gt = exports.le = exports.lt = exports.neq = exports.eq = exports._in = exports.or = exports.and = void 0;
const index_1 = require("./index");
var ConditionType;
(function (ConditionType) {
    ConditionType["EQ"] = "EQ";
    ConditionType["NEQ"] = "NEQ";
    ConditionType["LT"] = "LT";
    ConditionType["LE"] = "LE";
    ConditionType["GT"] = "GT";
    ConditionType["GE"] = "GE";
    ConditionType["IN"] = "IN";
    ConditionType["AND"] = "AND";
    ConditionType["OR"] = "OR";
})(ConditionType || (ConditionType = {}));
function toJSON(c) {
    return {
        "@class": c.class,
        "type": c.type,
    };
}
class SingleValueCondition {
    constructor(type, value) {
        this.type = type;
        this.value = value;
        this.class = index_1.PACKAGE + "dto.SingleValueConditionDto";
    }
    toJSON() {
        return Object.assign(Object.assign({}, toJSON(this)), { "value": this.value });
    }
}
class InCondition {
    constructor(values) {
        this.values = values;
        this.type = ConditionType.IN;
        this.class = index_1.PACKAGE + "dto.InConditionDto";
    }
    toJSON() {
        return Object.assign(Object.assign({}, toJSON(this)), { "values": this.values });
    }
}
class LogicalCondition {
    constructor(type, one, two) {
        this.type = type;
        this.one = one;
        this.two = two;
        this.class = index_1.PACKAGE + "dto.LogicalConditionDto";
    }
    toJSON() {
        return Object.assign(Object.assign({}, toJSON(this)), { "one": this.one, "two": this.two });
    }
}
function and(left, right) {
    return new LogicalCondition(ConditionType.AND, left, right);
}
exports.and = and;
function or(left, right) {
    return new LogicalCondition(ConditionType.OR, left, right);
}
exports.or = or;
function _in(value) {
    return new InCondition(value);
}
exports._in = _in;
function eq(value) {
    return new SingleValueCondition(ConditionType.EQ, value);
}
exports.eq = eq;
function neq(value) {
    return new SingleValueCondition(ConditionType.NEQ, value);
}
exports.neq = neq;
function lt(value) {
    return new SingleValueCondition(ConditionType.LT, value);
}
exports.lt = lt;
function le(value) {
    return new SingleValueCondition(ConditionType.LE, value);
}
exports.le = le;
function gt(value) {
    return new SingleValueCondition(ConditionType.GT, value);
}
exports.gt = gt;
function ge(value) {
    return new SingleValueCondition(ConditionType.GE, value);
}
exports.ge = ge;
