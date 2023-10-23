import PACKAGE from "./package";

export interface Order {
  readonly class: string
}

export enum OrderKeyword {
  ASC = "ASC",
  DESC = "DESC",
}

export class SimpleOrder implements Order {
  class: string = PACKAGE + "dto.SimpleOrderDto"

  constructor(private order: OrderKeyword) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "order": this.order,
    }
  }
}

export class ExplicitOrderDto implements Order {
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
