package me.paulbares.query.monitoring;

import com.google.common.base.Stopwatch;
import lombok.ToString;
import me.paulbares.query.Measure;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.function.Function.*;

@ToString
public class QueryWatch {

  public static final String GLOBAL = "global";
  public static final String PREPARE_PLAN = "prepare";
  public static final String PREPARE_RESOLVE_MEASURES = "resolveMeasures";
  public static final String PREPARE_CREATE_EXEC_PLAN = "createExecutionPlan";
  public static final String PREPARE_CREATE_QUERY_SCOPE = "createQueryScope";
  public static final String PREFETCH = "prefetch";
  public static final String BUCKET = "bucket";
  public static final String EXECUTE_PLAN = "execute";
  public static final String ORDER = "order";
  public static final Set<String> PREPARE_CHILDREN = Set.of(PREPARE_RESOLVE_MEASURES, PREPARE_CREATE_EXEC_PLAN, PREPARE_CREATE_QUERY_SCOPE);

  private Map<String, Stopwatch> stopwatches = new HashMap<>();

  private Map<Measure, Stopwatch> stopwatchByMeasure = new HashMap<>();

  public void start(String key) {
    this.stopwatches.computeIfAbsent(key, k -> Stopwatch.createStarted());
  }

  public void stop(String key) {
    this.stopwatches.get(key).stop();
  }

  public void start(Measure measure) {
    this.stopwatchByMeasure.computeIfAbsent(measure, k -> Stopwatch.createStarted());
  }

  public void stop(Measure measure) {
    this.stopwatchByMeasure.get(measure).stop();
  }

  public String toJson() {
    Map<String, Stopwatch> children = new LinkedHashMap<>();
    children.put(PREPARE_PLAN, this.stopwatches.get(PREPARE_PLAN));
    children.put(PREFETCH, this.stopwatches.get(PREFETCH));
    children.put(BUCKET, this.stopwatches.get(BUCKET));
    children.put(EXECUTE_PLAN, this.stopwatches.get(EXECUTE_PLAN));
    children.put(ORDER, this.stopwatches.get(ORDER));

    StringBuilder sb = new StringBuilder();
    addParentAndChildren(this,
            sb,
            this.stopwatches.get(GLOBAL),
            children,
            identity());
    return sb.toString();
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

    queryTimings.execute.total = this.stopwatches.get(EXECUTE_PLAN).elapsed(unit);
    for (Map.Entry<Measure, Stopwatch> e : this.stopwatchByMeasure.entrySet()) {
      String alias = e.getKey().alias();
      String key = alias == null ? e.getKey().expression() : alias;
      queryTimings.execute.detail.put(key, e.getValue().elapsed(unit));
    }

    return queryTimings;
  }

  private static <T> void addParentAndChildren(
          QueryWatch queryWatch,
          StringBuilder sb,
          Stopwatch parent,
          Map<T, Stopwatch> children,
          Function<T, String> keyPrinter) {
    sb.append('{'); // start obj
    addFieldWithColon(sb, "total");
    addField(sb, parent.toString());
    sb.append(',');
    addFieldWithColon(sb, "detail");
    sb.append('{'); // start obj detail

    var it = children.entrySet().iterator();
    while (it.hasNext()) {
      var e = it.next();
      T stopwatchKey = e.getKey();
      if (stopwatchKey.equals(PREPARE_PLAN)) {
        addFieldWithColon(sb, PREPARE_PLAN);
        Map<String, Stopwatch> collect = queryWatch.stopwatches.entrySet().stream()
                .filter(ee -> PREPARE_CHILDREN.contains(ee.getKey()))
                .collect(Collectors.toMap(ee -> ee.getKey(), ee -> ee.getValue()));
        addParentAndChildren(
                queryWatch,
                sb,
                e.getValue(),
                collect,
                identity());
      } else if (stopwatchKey.equals(EXECUTE_PLAN)) {
        addFieldWithColon(sb, EXECUTE_PLAN);
        addParentAndChildren(
                queryWatch,
                sb,
                e.getValue(),
                queryWatch.stopwatchByMeasure,
                m -> {
                  String alias = m.alias();
                  return alias == null ? m.expression() : alias;
                });
      } else {
        addFieldWithColon(sb, keyPrinter.apply(stopwatchKey));
        addField(sb, e.getValue().toString());
      }
      if (it.hasNext()) {
        sb.append(',');
      }
    }

    sb.append('}'); // end obj detail
    sb.append('}'); // end obj
  }

  public static void addField(StringBuilder sb, String field) {
    sb.append('"').append(field).append('"');
  }

  public static void addFieldWithColon(StringBuilder sb, String field) {
    sb.append('"').append(field).append('"').append(':');
  }
}
