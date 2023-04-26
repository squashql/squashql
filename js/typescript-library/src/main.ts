import {generateFromQueryDto} from "./generate-json-queryDto"
import {generateFromQuery} from "./generate-json-query";
import {generateFromQueryMerge} from "./generate-json-query-merge";
import {JoinType} from "./query";
import {from} from "./queryBuilder";
import {all, ConditionType, joinCriterion} from "./conditions";

generateFromQueryDto()
generateFromQuery()
generateFromQueryMerge()

const q = from("myTable")
        .join("refTable", JoinType.INNER)
        .on(all([
          joinCriterion("myTable.v", "refTable.min", ConditionType.GE),
          joinCriterion("myTable.v", "refTable.max", ConditionType.LT)
        ]))
        .select(["myTable.col", "refTable.col"], [], [])
        .build()
