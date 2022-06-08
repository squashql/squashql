package me.paulbares.query;

import me.paulbares.query.context.Repository;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodBucketingQueryDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.IsoFields;
import java.util.*;

public class PeriodBucketingExecutor {

  public final QueryEngine queryEngine;

  public PeriodBucketingExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Holder executeBucketing(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = buildPrefetchQuery(query);
    Table result = this.queryEngine.execute(prefetchQuery);

    List<Field> newColumns = getNewColumns(query.period);
    ObjectArrayDictionary dictionary = new ObjectArrayDictionary(query.coordinates.keySet().size() + newColumns.size());
    List<AggregatedMeasure> aggregatedMeasures = prefetchQuery.measures.stream().map(AggregatedMeasure.class::cast).toList();
    List<Field> aggregatedFields = result.headers().subList(result.headers().size() - aggregatedMeasures.size(), result.headers().size());
    Aggregator aggregator = new Aggregator(aggregatedMeasures, aggregatedFields);
    for (List<Object> row : result) {
      List<Object> leftColumnValues = row.subList(0, query.coordinates.keySet().size());
      List<Object> rightColumnValues = row.subList(query.coordinates.keySet().size(), row.size() - aggregatedMeasures.size());
      List<Object> aggregateValues = row.subList(row.size() - aggregatedMeasures.size(), row.size()); // align with aggregatedMeasures
      Object[] newColumnsValues = getNewColumnsValues(query.period, rightColumnValues);
      Object[] key = new Object[leftColumnValues.size() + newColumnsValues.length];
      for (int i = 0; i < key.length; i++) {
        if (i < leftColumnValues.size()) {
          key[i] = leftColumnValues.get(i);
        } else {
          key[i] = newColumnsValues[i - leftColumnValues.size()];
        }
      }
      aggregator.aggregate(dictionary.map(key), aggregateValues);
    }

    // Once the aggregation is done, build the table
    List<List<Object>> rows = new ArrayList<>();
    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>();
      Arrays.stream(points).forEach(p -> r.add(p));
      r.addAll(aggregator.aggregates.get(row));
      rows.add(r);
    });

    List<Field> originalColumns = result.headers().subList(0, query.coordinates.keySet().size());
    Table arrayTable = new ArrayTable(ImmutableListFactoryImpl.INSTANCE
            .withAll(originalColumns)
            .newWithAll(newColumns)
            .newWithAll(aggregatedFields)
            .castToList(),
            rows);

    return new Holder(arrayTable, originalColumns, newColumns, aggregatedFields, aggregatedMeasures, dictionary, aggregator);
  }

  public record Holder(Table table,
                       List<Field> originalColumns,
                       List<Field> newColumns,
                       List<Field> aggregatedFields,
                       List<AggregatedMeasure> aggregatedMeasures,
                       ObjectArrayDictionary dictionary,
                       Aggregator aggregator) {
  }

  public Table executeComparison(Holder bucketingResult, PeriodBucketingQueryDto periodBucketingQueryDto) {
    List<List<Object>> newRows = new ArrayList<>();
    ObjectArrayDictionary dictionary = bucketingResult.dictionary;
    Aggregator aggregator = bucketingResult.aggregator;
    int rowSize = bucketingResult.originalColumns.size() + bucketingResult.newColumns.size() + periodBucketingQueryDto.measures.size();
    Period period = periodBucketingQueryDto.period;
    ComparisonMeasure.PeriodUnit[] periodUnits = getPeriodUnits(period);

    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>(rowSize);
      Arrays.stream(points).forEach(p -> r.add(p));
      for (int i = 0; i < periodBucketingQueryDto.measures.size(); i++) {
        Measure measure = periodBucketingQueryDto.measures.get(i);
        if (measure instanceof ComparisonMeasure c) {
          AggregatedMeasure agg = c.measure;
          Object currentValue = aggregator.getAggregate(agg, row);

          Object[] referencePoint = new Object[bucketingResult.newColumns.size()];
          computeNewPositionFromReferencePosition(period, referencePoint, periodUnits, c.referencePosition);
//          c.
        } else {
          AggregatedMeasure agg = (AggregatedMeasure) measure;
        }
      }
//      r.addAll(aggregator.aggregates.get(row));
      newRows.add(r);
    });

    return null;
  }

  private QueryDto buildPrefetchQuery(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = new QueryDto()
            .table(query.table)
            .context(Repository.KEY, query.context.get(Repository.KEY));

    query.coordinates.keySet().forEach(c -> prefetchQuery.wildcardCoordinate(c));
    // Be sure to go down to the correct level of aggregation to be able to bucket
    getColumnsForPrefetching(query.period).forEach(c -> prefetchQuery.wildcardCoordinate(c));

    Set<AggregatedMeasure> set = new HashSet<>(); // use a set not to aggregate same measure multiple times
    for (Measure measure : query.measures) {
      if (measure instanceof AggregatedMeasure a) {
        set.add(a);
      } else if (measure instanceof ComparisonMeasure c) {
        set.add(c.measure);
      } else {
        throw new IllegalArgumentException(measure.getClass() + " type is not supported");
      }
    }
    set.forEach(a -> prefetchQuery.aggregatedMeasure(a.field, a.aggregationFunction));

    return prefetchQuery;
  }

  private List<String> getColumnsForPrefetching(Period period) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(q.year(), q.month());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  /**
   * Gets the list of new columns that will appear in the final result table once the bucketing is done.
   */
  private List<Field> getNewColumns(Period period) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(new Field(q.year(), String.class), new Field("quarter", String.class));
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  private ComparisonMeasure.PeriodUnit[] getPeriodUnits(Period period) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      return new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR, ComparisonMeasure.PeriodUnit.QUARTER};
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  public static Object[] computeNewPositionFromReferencePosition(
          Period period,
          Object[] position,
          ComparisonMeasure.PeriodUnit[] periodUnits,
          Map<ComparisonMeasure.PeriodUnit, String> referencePosition) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      Object[] result = Arrays.copyOf(position, position.length);
      Object[] transformations = new Object[position.length];

      for (int i = 0; i < periodUnits.length; i++) {
        ComparisonMeasure.PeriodUnit unit = periodUnits[i];
        String transformation = referencePosition.get(unit);
        if (transformation.contains("-")) {
          String[] split = transformation.split("-");
          Integer shift = Integer.valueOf(split[1].trim());
          transformations[i] = -shift;
        } else if (transformation.contains("+")) {
          String[] split = transformation.split("\\+");
          Integer shift = Integer.valueOf(split[1].trim());
          transformations[i] = shift;
        } else {
          // nothing
        }
      }

      // YEAR, QUARTER
      int year = (int) position[0];
      if (referencePosition.containsKey(ComparisonMeasure.PeriodUnit.YEAR)) {
        if (transformations[0] != null) {
          result[0] = year + (int) transformations[0];
        }
      }
      if (referencePosition.containsKey(ComparisonMeasure.PeriodUnit.YEAR)) {
        int quarter = (int) position[1];
        if (transformations[1] != null) {
          LocalDate d = LocalDate.of(year, quarter * 3, 1);
          LocalDate newd = d.plusMonths(((int) transformations[1]) * 3);
          result[1] = (int) IsoFields.QUARTER_OF_YEAR.getFrom(newd);
          result[0] = newd.getYear(); // year might have changed
        }
      }

      return result;
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  private Object[] getNewColumnsValues(Period period, List<Object> args) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      // args must be of size 2 and contain [year, month]. 1 <= month <= 2
      return new Object[]{args.get(0), (int) IsoFields.QUARTER_OF_YEAR.getFrom(Month.of((Integer) args.get(1)))};
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  private static class Aggregator {

    private final ObjectIntHashMap<AggregatedMeasure> measureByIndex;
    private final List<Field> fields;
    private final List<List<Object>> aggregates;

    private Aggregator(List<AggregatedMeasure> measures, List<Field> fields) {
      this.fields = fields;
      this.aggregates = new ArrayList<>();

      this.measureByIndex = new ObjectIntHashMap<>();
      for (int i = 0; i < fields.size(); i++) {
        AggregatedMeasure m = measures.get(i);
        this.measureByIndex.put(m, i);
        Field f = fields.get(i);
        if (!m.aggregationFunction.equals("sum")) {
          throw new IllegalStateException("Aggregation function " + m.aggregationFunction + " not supported. Only sum is supported");
        }
        if (!isTypeSupported(f)) {
          throw new IllegalStateException("Type " + f.type() + " is not supported");
        }
      }

    }

    void aggregate(int rowIndex, List<Object> values) {
      if (rowIndex >= this.aggregates.size()) {
        this.aggregates.add(new ArrayList<>(values));
      } else {
        List<Object> oldValues = this.aggregates.get(rowIndex);
        for (int i = 0; i < values.size(); i++) {
          Field f = fields.get(i);

          if (f.type().equals(double.class)) {
            oldValues.set(i, (double) oldValues.get(i) + (double) values.get(i));
          } else if (f.type().equals(long.class)) {
            oldValues.set(i, (long) oldValues.get(i) + (long) values.get(i));
          } else if (f.type().equals(int.class)) {
            oldValues.set(i, (int) oldValues.get(i) + (int) values.get(i));
          }
        }
      }
    }

    public Object getAggregate(AggregatedMeasure measure, int rowIndex) {
      return this.aggregates.get(rowIndex).get(this.measureByIndex.get(measure));
    }

    private boolean isTypeSupported(Field field) {
      Class<?> t = field.type();
      return t.equals(double.class) || t.equals(Double.class)
              || t.equals(int.class) || t.equals(Integer.class)
              || t.equals(long.class) || t.equals(Long.class);
    }
  }
}
