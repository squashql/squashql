"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BinaryOperator = exports.BinaryOperationMeasure = exports.ExpressionMeasure = exports.AggregatedMeasure = void 0;
const index_1 = require("./index");
class AggregatedMeasure {
    constructor(field, aggregationFunction, alias, conditionField, condition) {
        this.class = index_1.PACKAGE + "AggregatedMeasure";
        this.field = field;
        this.aggregationFunction = aggregationFunction;
        this.alias = alias;
        this.conditionField = conditionField;
        this.condition = condition;
    }
    toJSON() {
        return {
            "@class": this.class,
            "field": this.field,
            "aggregationFunction": this.aggregationFunction,
            "alias": this.alias,
            "expression": this.expression,
            "conditionField": this.conditionField,
            "conditionDto": this.condition,
        };
    }
}
exports.AggregatedMeasure = AggregatedMeasure;
class ExpressionMeasure {
    constructor(alias, sqlExpression) {
        this.alias = alias;
        this.sqlExpression = sqlExpression;
        this.class = index_1.PACKAGE + "ExpressionMeasure";
    }
    toJSON() {
        return {
            "@class": this.class,
            "alias": this.alias,
            "expression": this.sqlExpression,
        };
    }
}
exports.ExpressionMeasure = ExpressionMeasure;
class BinaryOperationMeasure {
    constructor(alias, operator, leftOperand, rightOperand) {
        this.class = index_1.PACKAGE + "BinaryOperationMeasure";
        this.alias = alias;
        this.operator = operator;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }
    toJSON() {
        return {
            "@class": this.class,
            "alias": this.alias,
            "operator": this.operator,
            "leftOperand": this.leftOperand,
            "rightOperand": this.rightOperand,
        };
    }
}
exports.BinaryOperationMeasure = BinaryOperationMeasure;
var BinaryOperator;
(function (BinaryOperator) {
    BinaryOperator["PLUS"] = "PLUS";
    BinaryOperator["MINUS"] = "MINUS";
    BinaryOperator["MULTIPLY"] = "MULTIPLY";
    BinaryOperator["DIVIDE"] = "DIVIDE";
})(BinaryOperator = exports.BinaryOperator || (exports.BinaryOperator = {}));
