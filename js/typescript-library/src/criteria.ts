import {Field} from "./field"
import {BasicMeasure} from "./measure"
import {Condition, ConditionType} from "./condition"
import PACKAGE from "./package"

export default class Criteria {
  readonly class: string = PACKAGE + "dto.CriteriaDto"

  constructor(public field: Field,
              public fieldOther: Field,
              public measure: BasicMeasure,
              public condition: Condition,
              public conditionType: ConditionType,
              public children: Criteria[]) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "field": this.field,
      "fieldOther": this.fieldOther,
      "measure": this.measure,
      "condition": this.condition,
      "conditionType": this.conditionType,
      "children": this.children,
    }
  }
}
