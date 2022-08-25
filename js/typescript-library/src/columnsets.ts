import {PACKAGE} from "./index";

export interface ColumnSet {
  readonly class: string
}

class BucketColumnSet {
  class: string = PACKAGE + "BucketColumnSetDto"

  constructor(private name: string, private field: string, private values: Map<string, Array<string>>) {
    this.values = new Map<string, Array<string>>();
  }
}

class PeriodColumnSet {
  class: string = PACKAGE + "PeriodColumnSetDto"

  constructor(private period: Period) {
  }
}

export interface Period {
}

class Month implements Period {

  constructor(private month: string, private year: string) {
  }
}

class Quarter implements Period {

  constructor(private quarter: string, private year: string) {
  }
}

class Semester implements Period {

  constructor(private semester: string, private year: string) {
  }
}

class Year implements Period {

  constructor(private year: string) {
  }
}

