package me.paulbares.query;

import me.paulbares.query.agg.SumAggregator;
import me.paulbares.query.comp.Comparisons;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodBucketingQueryDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class PeriodBucketingExecutor {

  public final QueryEngine queryEngine;

  public PeriodBucketingExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(PeriodBucketingQueryDto query) {
    Holder holder = executeBucketing(query);
    return executeComparison(holder, query);
  }

  public Holder executeBucketing(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = buildPrefetchQuery(query);
    Table result = this.queryEngine.execute(prefetchQuery);

    List<Field> newColumns = getNewColumns(query.period);
    ObjectArrayDictionary dictionary = new ObjectArrayDictionary(query.coordinates.keySet().size() + newColumns.size());
    List<AggregatedMeasure> aggregatedMeasures = prefetchQuery.measures.stream().map(AggregatedMeasure.class::cast).toList();
    List<Field> aggregatedFields = result.headers().subList(result.headers().size() - aggregatedMeasures.size(), result.headers().size());
    SumAggregator aggregator = new SumAggregator(aggregatedMeasures, aggregatedFields);
    for (List<Object> row : result) {
      List<Object> originalColumnValues = row.subList(0, query.coordinates.keySet().size());
      List<Object> toBucketColumnValues = row.subList(query.coordinates.keySet().size(), row.size() - aggregatedMeasures.size());
      List<Object> aggregateValues = row.subList(row.size() - aggregatedMeasures.size(), row.size()); // align with aggregatedMeasures
      Object[] bucketValues = getBucketValues(query.period, toBucketColumnValues);
      Object[] key = new Object[originalColumnValues.size() + bucketValues.length];
      for (int i = 0; i < key.length; i++) {
        if (i < originalColumnValues.size()) {
          key[i] = originalColumnValues.get(i);
        } else {
          key[i] = bucketValues[i - originalColumnValues.size()];
        }
      }
      aggregator.aggregate(dictionary.map(key), aggregateValues);
    }

    // Once the aggregation is done, build the table
    List<List<Object>> rows = new ArrayList<>();
    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>();
      r.addAll(Arrays.asList(points));
      r.addAll(aggregator.getAggregates(row));
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
                       SumAggregator aggregator) {
  }

  public Table executeComparison(Holder bucketingResult, PeriodBucketingQueryDto periodBucketingQueryDto) {
    List<List<Object>> newRows = new ArrayList<>();
    ObjectArrayDictionary dictionary = bucketingResult.dictionary;
    SumAggregator aggregator = bucketingResult.aggregator;
    int rowSize = bucketingResult.originalColumns.size() + bucketingResult.newColumns.size() + periodBucketingQueryDto.measures.size();
    Period period = periodBucketingQueryDto.period;
    ComparisonMeasure.PeriodUnit[] periodUnits = getPeriodUnits(period);

    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>(rowSize);
      r.addAll(Arrays.asList(points));
      for (int i = 0; i < periodBucketingQueryDto.measures.size(); i++) {
        Measure measure = periodBucketingQueryDto.measures.get(i);
        if (measure instanceof ComparisonMeasure c) {
          AggregatedMeasure agg = c.measure;

          Object[] point = new Object[bucketingResult.newColumns.size()]; // FIXME we can create a buffer here
          System.arraycopy(points, bucketingResult.originalColumns.size(), point, 0, point.length);
          Object[] shiftPoint = computeNewPositionFromReferencePosition(period, point, periodUnits, c.referencePosition);

          Object[] referencePosition = new Object[points.length];
          System.arraycopy(points, 0, referencePosition, 0, bucketingResult.originalColumns.size());
          System.arraycopy(shiftPoint, 0, referencePosition, bucketingResult.originalColumns.size(), shiftPoint.length);

          int position = dictionary.getPosition(referencePosition);
          if (position != -1) {
            Object currentValue = aggregator.getAggregate(agg, row);
            Object referenceValue = aggregator.getAggregate(agg, position);
            Object diff = Comparisons.compare(c.method, currentValue, referenceValue, aggregator.getField(agg).type());
            r.add(diff);
          } else {
            r.add(null); // nothing to compare with
          }
        } else {
          // Simple measure, recopy the value
          r.add(aggregator.getAggregate((AggregatedMeasure) measure, row));
        }
      }
      newRows.add(r);
    });

    List<Field> measureFields = new ArrayList<>();
    for (Measure measure : periodBucketingQueryDto.measures) {
      if (measure instanceof AggregatedMeasure a) {
        measureFields.add(aggregator.getField(a));
      } else if (measure instanceof ComparisonMeasure c) {
        String newName = c.alias == null
                ? String.format("%s(%s, %s)", c.method, c.measure.alias(), c.referencePosition)
                : c.alias;
        measureFields.add(new Field(newName, Comparisons.getOutputType(c.method, aggregator.getField(c.measure).type())));
      } else {
        throw new IllegalArgumentException(measure.getClass() + " type is not supported");
      }
    }

    return new ArrayTable(ImmutableListFactoryImpl.INSTANCE
            .withAll(bucketingResult.originalColumns)
            .newWithAll(bucketingResult.newColumns)
            .newWithAll(measureFields)
            .castToList(),
            newRows);
  }

  private QueryDto buildPrefetchQuery(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = new QueryDto()
            .table(query.table)
            .context(Repository.KEY, query.context.get(Repository.KEY));

    query.coordinates.keySet().forEach(prefetchQuery::wildcardCoordinate);
    // Be sure to go down to the correct level of aggregation to be able to bucket
    getColumnsForPrefetching(query.period).forEach(prefetchQuery::wildcardCoordinate);

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

  /**
   * Gets the column names to use for prefetching. It will determine which grouping of aggregates are needed to further
   * perform the bucketing.
   */
  private List<String> getColumnsForPrefetching(Period period) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(q.year(), q.month());
    } else if (period instanceof Period.QuarterFromDate q) {
      return List.of(q.date());
    } else if (period instanceof Period.YearFromDate y) {
      return List.of(y.date());
    } else if (period instanceof Period.Year y) {
      return List.of(y.year());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  /**
   * Gets the list of new fields that will appear in the final result table once the bucketing is done.
   */
  private List<Field> getNewColumns(Period period) {
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(new Field(q.year(), String.class), new Field("quarter", String.class));
    } else if (period instanceof Period.QuarterFromDate) {
      return List.of(new Field("year", String.class), new Field("quarter", String.class));
    } else if (period instanceof Period.YearFromDate) {
      return List.of(new Field("year", String.class));
    } else if (period instanceof Period.Year y) {
      return List.of(new Field(y.year(), String.class));
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  private ComparisonMeasure.PeriodUnit[] getPeriodUnits(Period period) {
    if (period instanceof Period.QuarterFromMonthYear) {
      return new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR, ComparisonMeasure.PeriodUnit.QUARTER};
    } else if (period instanceof Period.QuarterFromDate) {
      return new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR, ComparisonMeasure.PeriodUnit.QUARTER};
    } else if (period instanceof Period.YearFromDate || period instanceof Period.Year) {
      return new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR};
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  // FIXME API is confusing
  public static Object[] computeNewPositionFromReferencePosition(
          Period period,
          Object[] position,
          ComparisonMeasure.PeriodUnit[] periodUnits,
          Map<ComparisonMeasure.PeriodUnit, String> referencePosition) {
    // Compute the transformations
    Object[] transformations = new Object[position.length];
    for (int i = 0; i < periodUnits.length; i++) {
      ComparisonMeasure.PeriodUnit unit = periodUnits[i];
      String transformation = referencePosition.get(unit);
      if (transformation.contains("-")) {
        String[] split = transformation.split("-");
        int shift = Integer.parseInt(split[1].trim());
        transformations[i] = -shift;
      } else if (transformation.contains("+")) {
        String[] split = transformation.split("\\+");
        int shift = Integer.parseInt(split[1].trim());
        transformations[i] = shift;
      }
      // else nothing
    }

    if (period instanceof Period.QuarterFromMonthYear || period instanceof Period.QuarterFromDate) {
      // YEAR, QUARTER
      int year = (int) position[0];
      if (referencePosition.containsKey(ComparisonMeasure.PeriodUnit.YEAR)) {
        if (transformations[0] != null) {
          position[0] = year + (int) transformations[0];
        }
      }
      if (referencePosition.containsKey(ComparisonMeasure.PeriodUnit.QUARTER)) {
        int quarter = (int) position[1];
        if (transformations[1] != null) {
          LocalDate d = LocalDate.of((Integer) position[0], quarter * 3, 1);
          LocalDate newd = d.plusMonths(((int) transformations[1]) * 3);
          position[1] = (int) IsoFields.QUARTER_OF_YEAR.getFrom(newd);
          position[0] = newd.getYear(); // year might have changed
        }
      }
    } else if (period instanceof Period.YearFromDate || period instanceof Period.Year) {
      // YEAR
      int year = (int) position[0];
      if (referencePosition.containsKey(ComparisonMeasure.PeriodUnit.YEAR)) {
        if (transformations[0] != null) {
          position[0] = year + (int) transformations[0];
        }
      }
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
    return position;
  }

  private Object[] getBucketValues(Period period, List<Object> args) {
    if (period instanceof Period.QuarterFromMonthYear) {
      assert args.size() == 2;
      // args must be of size 2 and contain [year, month]. 1 <= month <= 2
      return new Object[]{args.get(0), (int) IsoFields.QUARTER_OF_YEAR.getFrom(Month.of((Integer) args.get(1)))};
    } else if (period instanceof Period.QuarterFromDate) {
      assert args.size() == 1;
      TemporalAccessor date = (TemporalAccessor) args.get(0);
      return new Object[]{date.get(ChronoField.YEAR), (int) IsoFields.QUARTER_OF_YEAR.getFrom(date)};
    } else if (period instanceof Period.YearFromDate) {
      assert args.size() == 1;
      TemporalAccessor date = (TemporalAccessor) args.get(0);
      return new Object[]{date.get(ChronoField.YEAR)};
    } else if (period instanceof Period.Year) {
      assert args.size() == 1;
      return new Object[]{args.get(0)};
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }
}