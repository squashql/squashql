import {generateFromQueryDto} from "./__tests__/generate-json-queryDto"
import {generateJsonQuery} from "./__tests__/generate-json-query"
import {generateFromQueryMerge} from "./__tests__/generate-json-query-merge"
import {generateFromQueryPivot} from "./__tests__/generate-json-query-pivot"
import {generateFromQueryMergePivot} from "./__tests__/generate-json-query-merge-pivot"
import {generateFromQueryJoin} from "./__tests__/generate-json-query-join"
import {generateQueryResults} from "./__tests__/generate-json-query-results"

generateFromQueryDto()
generateJsonQuery()
generateFromQueryMerge()
generateFromQueryPivot()
generateFromQueryMergePivot()
generateFromQueryJoin()
generateQueryResults()
