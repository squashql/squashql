import PACKAGE from "./package";

export interface Parameter {
  readonly class: string
  readonly key: string
}

export enum Action {
  USE = "USE", NOT_USE = "NOT_USE", INVALIDATE = "INVALIDATE"
}

export class QueryCacheParameter implements Parameter {
  readonly class: string = PACKAGE + "parameter.QueryCacheParameter"
  readonly key: string = "cache"

  constructor(private action: Action) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "action": this.action,
    }
  }
}
