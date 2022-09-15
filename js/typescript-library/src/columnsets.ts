import {PACKAGE} from "./index";

export interface ColumnSet {
  readonly class: string
}

export enum ColumnSetKey {
  BUCKET = "BUCKET",
  PERIOD = "PERIOD",
}

export class BucketColumnSet {
  class: string = PACKAGE + "dto.BucketColumnSetDto"

  constructor(private name: string, private field: string, private values: Map<string, Array<string>>) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "name": this.name,
      "field": this.field,
      "values": Object.fromEntries(this.values),
    }
  }
}

export class PeriodColumnSet {
  class: string = PACKAGE + "dto.PeriodColumnSetDto"

  constructor(private period: Period) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "period": this.period,
    }
  }
}

export interface Period {
  readonly class: string,
}

export class Month implements Period {
  class: string = PACKAGE + "dto.Period$Month"

  constructor(private month: string, private year: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "month": this.month,
      "year": this.year,
    }
  }
}

export class Quarter implements Period {
  class: string = PACKAGE + "dto.Period$Quarter"

  constructor(private quarter: string, private year: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "quarter": this.quarter,
      "year": this.year,
    }
  }
}

export class Semester implements Period {
  class: string = PACKAGE + "dto.Period$Semester"

  constructor(private semester: string, private year: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "semester": this.semester,
      "year": this.year,
    }
  }
}

export class Year implements Period {
  class: string = PACKAGE + "dto.Period$Year"

  constructor(private year: string) {
  }

  toJSON() {
    return {
      "@class": this.class,
      "year": this.year,
    }
  }
}

