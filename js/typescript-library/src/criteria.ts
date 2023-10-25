import {Field} from "./types/field";
import {BasicMeasure} from "./types/measure";
import {Condition, ConditionType} from "./types/conditions";

export default class Criteria {

  constructor(public field: Field,
              public fieldOther: Field,
              private measure: BasicMeasure,
              private condition: Condition,
              public conditionType: ConditionType,
              public children: Criteria[]) {
  }
}
