import {BasicMeasure, Condition, ConditionType, Field} from "./types";

export default class Criteria {

  constructor(public field: Field,
              public fieldOther: Field,
              private measure: BasicMeasure,
              private condition: Condition,
              public conditionType: ConditionType,
              public children: Criteria[]) {
  }
}
