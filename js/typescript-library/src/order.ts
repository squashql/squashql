import PACKAGE from "./package"

export interface Order {
  readonly class: string
}

export enum OrderKeyword {
  ASC = "ASC",
  DESC = "DESC",
}

export enum NullsOrderKeyword {
  FIRST = "FIRST",
  LAST = "LAST",
}

export class SimpleOrder implements Order {
  class: string = PACKAGE + "dto.SimpleOrderDto"

  constructor(private order: OrderKeyword, private nullsOrder: NullsOrderKeyword = null) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "order": this.order,
      "nullsOrder": this.nullsOrder,
    }
  }
}

export class ExplicitOrder implements Order {
  class: string = PACKAGE + "dto.ExplicitOrderDto"

  constructor(private explicit: Array<any>) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "explicit": this.explicit,
    }
  }
}
