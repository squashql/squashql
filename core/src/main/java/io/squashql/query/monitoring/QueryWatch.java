package io.squashql.query.monitoring;

import com.google.common.base.Stopwatch;
import io.squashql.query.QueryExecutor.QueryPlanNodeKey;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@ToString
public class QueryWatch {

  public static final String GLOBAL = "global";
  public static final String PREPARE_PLAN = "prepare";
  public static final String PREPARE_RESOLVE_MEASURES = "resolveMeasures";
  public static final String EXECUTE_PREFETCH_PLAN = "executePrefetchPlan";
  public static final String PREFETCH = "prefetch";
  public static final String BUCKET = "bucket";
  public static final String EXECUTE_EVALUATION_PLAN = "executeEvaluationPlan";
  public static final String ORDER = "order";
  public static final Set<String> PREPARE_CHILDREN = Set.of(PREPARE_RESOLVE_MEASURES, EXECUTE_PREFETCH_PLAN);

  private Map<String, Stopwatch> stopwatches = new HashMap<>();

  private Map<QueryPlanNodeKey, Stopwatch> stopwatchByMeasure = new HashMap<>();

  public void start(String key) {
    this.stopwatches.computeIfAbsent(key, k -> Stopwatch.createStarted());
  }

  public void stop(String key) {
    this.stopwatches.get(key).stop();
  }

  public void start(QueryPlanNodeKey queryPlanNodeKey) {
    this.stopwatchByMeasure.computeIfAbsent(queryPlanNodeKey, k -> Stopwatch.createStarted());
  }

  public void stop(QueryPlanNodeKey queryPlanNodeKey) {
    this.stopwatchByMeasure.get(queryPlanNodeKey).stop();
  }

  public QueryTimings toQueryTimings() {
    QueryTimings queryTimings = new QueryTimings();
    TimeUnit unit = TimeUnit.MICROSECONDS;
    queryTimings.total = this.stopwatches.get(GLOBAL).elapsed(unit);

    queryTimings.bucket = this.stopwatches.get(BUCKET).elapsed(unit);
    queryTimings.prefetch = this.stopwatches.get(PREFETCH).elapsed(unit);
    queryTimings.order = this.stopwatches.get(ORDER).elapsed(unit);

    queryTimings.prepare.total = this.stopwatches.get(PREPARE_PLAN).elapsed(unit);
    for (String prepareChild : PREPARE_CHILDREN) {
      queryTimings.prepare.detail.put(prepareChild, this.stopwatches.get(prepareChild).elapsed(unit));
    }

    queryTimings.execute.total = this.stopwatches.get(EXECUTE_EVALUATION_PLAN).elapsed(unit);
    for (Map.Entry<QueryPlanNodeKey, Stopwatch> e : this.stopwatchByMeasure.entrySet()) {
      String key = e.getKey().measure().alias();
      queryTimings.execute.detail.put(key, e.getValue().elapsed(unit));
    }

    return queryTimings;
  }
}
