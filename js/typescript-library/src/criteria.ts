import {Field} from "./field"
import {BasicMeasure} from "./measure"
import {Condition, ConditionType} from "./conditions"

export default class Criteria {

  constructor(public field: Field,
              public fieldOther: Field,
              private measure: BasicMeasure,
              private condition: Condition,
              public conditionType: ConditionType,
              public children: Criteria[]) {
  }
}
