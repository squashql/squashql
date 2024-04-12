import {count, ExpressionMeasure, TableField} from "./index"

// export const countRows = new AggregatedMeasure("_contributors_count_", new TableField("*"), "count")
export const countRows = count("_contributors_count_", new TableField("*"))
export const totalCount = new ExpressionMeasure("_total_count_", "COUNT(*) OVER ()")
