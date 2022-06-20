package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.query.dto.*;
import me.paulbares.store.Field;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.util.*;

public class NewQueryExecutor {
  // FIXME conditions are ignored for the timebeing but should be taken into account in the future.
  private Map<List<String>, Set<Measure>> measuresByDepth = new HashMap<>();

  public final QueryEngine queryEngine;

  public NewQueryExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  // if bucket, and not BinaryOperationMeasure with reference_position using buckets, it is a copy.
  public Table execute(NewQueryDto query) {
    // Collect columns
    List<String> cols = new ArrayList<>(query.columns);
    query.columnSets.values().forEach(cs -> cols.addAll(cs.getColumnsForPrefetching()));

    // The final scope:
    List<String> scope = new ArrayList<>(query.columns);
    // Make sure bucket is first, period is after
    query.columnSets.values().stream().sorted((c1, c2) -> {
      if (c1 instanceof BucketColumnSetDto) {
        return -1;
      } else if (c2 instanceof BucketColumnSetDto) {
        return 1;
      } else {
        return -1; // we don't care
      }
    }).forEach(cs -> scope.addAll(cs.getNewColumns().stream().map(Field::name).toList()));

    // Final result should contain the following fields: [scope, query.measures]

    List<Measure> measures = query.measures;

    Table t;
    if (query.columnSets.containsKey(NewQueryDto.PERIOD)) {
      PeriodColumnSetDto columnSet = (PeriodColumnSetDto) query.columnSets.get(NewQueryDto.PERIOD);
      PeriodBucketingExecutor pbe = new PeriodBucketingExecutor(this.queryEngine);
      PeriodBucketingQueryDto periodBucketingQueryDto = new PeriodBucketingQueryDto()
              .table(query.table)
              .period(columnSet.period);
      List<String> colsCopy = new ArrayList<>(cols);
      colsCopy.removeAll(columnSet.getColumnsForPrefetching());
      colsCopy.forEach(periodBucketingQueryDto::wildcardCoordinate);
      measures.forEach(periodBucketingQueryDto::withMeasure);
      Bucketer.Holder holder = pbe.executeBucketing(periodBucketingQueryDto);
      System.out.println();
      t = holder.table();
      // output scope is "scope"
    } else {
      // Regular
      QueryDto q = new QueryDto().table(query.table);
      cols.forEach(q::wildcardCoordinate);
      measures.forEach(q::withMeasure);
      t = this.queryEngine.execute(q);
    }

    if (query.columnSets.containsKey(NewQueryDto.BUCKET)) {
      // Now bucket...
      BucketColumnSetDto columnSet = (BucketColumnSetDto) query.columnSets.get(NewQueryDto.BUCKET);
      Map<String, List<String>> bucketsByValue = new HashMap<>();
      for (Pair<String, List<String>> value : columnSet.values) {
        for (String v : value.getTwo()) {
          bucketsByValue
                  .computeIfAbsent(v, k -> new ArrayList<>())
                  .add(value.getOne());
        }
      }

      List<String> r = t.headers().stream().map(Field::name).toList();
      int[] indexColsToBucket = new int[1];
      int[] indexColAggregates = t.measureIndices();
      MutableIntList indexColsToLeaveList = new IntArrayList();
      for (int i = 0; i < r.size(); i++) {
        if (columnSet.getColumnsForPrefetching().contains(r.get(i))) {
          indexColsToBucket[0] = i;
        } else if (Arrays.binarySearch(indexColAggregates, i) < 0) {
          indexColsToLeaveList.add(i);
        }
      }
      int[] indexColsToLeave = indexColsToLeaveList.toArray();

      Bucketer.Holder holder = new Bucketer(this.queryEngine)
              .executeBucketing(t,
                      indexColsToLeave,
                      indexColAggregates,
                      indexColsToBucket,
                      t.measures().stream().map(AggregatedMeasure.class::cast).toList(),
                      columnSet.getNewColumns(),
                      toBucketColumnValues -> {
                        String value = (String) toBucketColumnValues.get(0);
                        List<String> buckets = bucketsByValue.get(value);
                        return buckets.stream().map(b -> new Object[]{b, value}).toList();
                      });
      t = holder.table();
      System.out.println();
    }


    // TODO apply period first then bucket

    Set<Measure> set = new HashSet<>(); // use a set not to aggregate same measure multiple times
    for (Measure measure : query.measures) {
      if (measure instanceof AggregatedMeasure a) {
        set.add(a); // TODO what todo if bucketing?
      } else if (measure instanceof BinaryOperationMeasure c) {
        if (!(c.measure instanceof BinaryOperationMeasure)) {
          set.add(c.measure);
        } else {
          BinaryOperationMeasure bom = (BinaryOperationMeasure) c.measure;
          if (!(bom.measure instanceof AggregatedMeasure)) {
            throw new RuntimeException("not supported"); // FIXME fix the logic, it is flaky
          }
          set.add(bom.measure);
        }
      } else {
        throw new IllegalArgumentException(measure.getClass() + " type is not supported");
      }
    }

    return t;
  }
}
