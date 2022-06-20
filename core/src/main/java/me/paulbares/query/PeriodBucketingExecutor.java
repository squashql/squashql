package me.paulbares.query;

import me.paulbares.query.agg.SumAggregator;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dictionary.ObjectArrayDictionary;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodBucketingQueryDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.list.immutable.ImmutableListFactoryImpl;

import java.time.LocalDate;
import java.time.Month;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class PeriodBucketingExecutor {

  public final QueryEngine queryEngine;

  public PeriodBucketingExecutor(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  public Table execute(PeriodBucketingQueryDto query) {
    Bucketer.Holder holder = executeBucketing(query);
    return executeComparison(holder, query);
  }

  public Bucketer.Holder executeBucketing(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = buildPrefetchQuery(query);
    Table result = this.queryEngine.execute(prefetchQuery);

    int[] indexColsToLeave = IntStream.range(0, query.coordinates.keySet().size()).toArray();
    int[] indexColAggregates = IntStream.range(prefetchQuery.coordinates.keySet().size(), result.headers().size()).toArray();
    int[] indexColsToBucket = IntStream.range(query.coordinates.keySet().size(), prefetchQuery.coordinates.keySet().size()).toArray();
    List<AggregatedMeasure> aggregatedMeasures = prefetchQuery.measures.stream().map(AggregatedMeasure.class::cast).toList();

    return new Bucketer(this.queryEngine)
            .executeBucketing(result,
                    indexColsToLeave,
                    indexColAggregates,
                    indexColsToBucket,
                    aggregatedMeasures,
                    PeriodColumnSetDto.getNewColumns(query.period),
                    toBucketColumnValues -> Collections.singletonList(getBucketValues(query.period, toBucketColumnValues)));
  }

  public Table executeComparison(Bucketer.Holder bucketingResult, PeriodBucketingQueryDto query) {
    List<List<Object>> newRows = new ArrayList<>();
    ObjectArrayDictionary dictionary = bucketingResult.dictionary();
    SumAggregator aggregator = bucketingResult.aggregator();
    ShiftProcedure[] procedures = new ShiftProcedure[query.measures.size()];
    for (int i = 0; i < procedures.length; i++) {
      if (query.measures.get(i) instanceof BinaryOperationMeasure c) {
        Map<BinaryOperationMeasure.PeriodUnit, String> m = new HashMap<>();
        for (Map.Entry<String, String> entry : c.referencePosition.entrySet()) {
          m.put(BinaryOperationMeasure.PeriodUnit.valueOf(entry.getKey()), entry.getValue());
        }
        procedures[i] = new ShiftProcedure(query.period, m, bucketingResult.newColumns().size());
      }
    }
    int rowSize = bucketingResult.originalColumns().size() + bucketingResult.newColumns().size() + query.measures.size();
    Object[] buffer = new Object[bucketingResult.newColumns().size()];
    Object[] referencePositionBuffer = new Object[dictionary.getPointLength()];
    dictionary.forEach((points, row) -> {
      List<Object> r = new ArrayList<>(rowSize);
      r.addAll(Arrays.asList(points));
      for (int i = 0; i < query.measures.size(); i++) {
        Measure measure = query.measures.get(i);
        if (measure instanceof BinaryOperationMeasure c) {
          System.arraycopy(points, bucketingResult.originalColumns().size(), buffer, 0, buffer.length);
          procedures[i].execute(buffer);

          System.arraycopy(points, 0, referencePositionBuffer, 0, bucketingResult.originalColumns().size());
          System.arraycopy(buffer, 0, referencePositionBuffer, bucketingResult.originalColumns().size(), buffer.length);

          int position = dictionary.getPosition(referencePositionBuffer);
          if (position != -1) {
            AggregatedMeasure agg = (AggregatedMeasure) c.measure;
            Object currentValue = aggregator.getAggregate(agg, row);
            Object referenceValue = aggregator.getAggregate(agg, position);
            Object diff = BinaryOperations.compare(c.method, currentValue, referenceValue, aggregator.getField(agg).type());
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
    for (Measure measure : query.measures) {
      if (measure instanceof AggregatedMeasure a) {
        measureFields.add(aggregator.getField(a));
      } else if (measure instanceof BinaryOperationMeasure c) {
        String newName = c.alias == null
                ? String.format("%s(%s, %s)", c.method, c.measure.alias(), c.referencePosition)
                : c.alias;
        measureFields.add(new Field(newName, BinaryOperations.getOutputType(c.method, aggregator.getField((AggregatedMeasure) c.measure).type())));
      } else {
        throw new IllegalArgumentException(measure.getClass() + " type is not supported");
      }
    }

    int columnSize = bucketingResult.originalColumns().size() + bucketingResult.newColumns().size();
    return new ArrayTable(ImmutableListFactoryImpl.INSTANCE
            .withAll(bucketingResult.originalColumns())
            .newWithAll(bucketingResult.newColumns())
            .newWithAll(measureFields)
            .castToList(),
            query.measures,
            IntStream.range(columnSize, columnSize + measureFields.size()).toArray(),
            newRows);
  }

  private QueryDto buildPrefetchQuery(PeriodBucketingQueryDto query) {
    QueryDto prefetchQuery = new QueryDto()
            .table(query.table)
            .context(Repository.KEY, query.context.get(Repository.KEY));

    query.coordinates.keySet().forEach(prefetchQuery::wildcardCoordinate);
    // Be sure to go down to the correct level of aggregation to be able to bucket
    PeriodColumnSetDto.getColumnsForPrefetching(query.period).forEach(prefetchQuery::wildcardCoordinate);

    Set<AggregatedMeasure> set = new HashSet<>(); // use a set not to aggregate same measure multiple times
    for (Measure measure : query.measures) {
      if (measure instanceof AggregatedMeasure a) {
        set.add(a);
      } else if (measure instanceof BinaryOperationMeasure c) {
        set.add((AggregatedMeasure) c.measure);
      } else {
        throw new IllegalArgumentException(measure.getClass() + " type is not supported");
      }
    }
    set.forEach(a -> prefetchQuery.aggregatedMeasure(a.field, a.aggregationFunction));

    return prefetchQuery;
  }

  static class ShiftProcedure {

    final Object[] transformations;
    final Period period;
    final Map<BinaryOperationMeasure.PeriodUnit, String> referencePosition;

    ShiftProcedure(Period period, Map<BinaryOperationMeasure.PeriodUnit, String> referencePosition, int pointLength) {
      this.period = period;
      this.referencePosition = referencePosition;
      this.transformations = new Object[pointLength];
      BinaryOperationMeasure.PeriodUnit[] periodUnits = getPeriodUnits(period);
      for (int i = 0; i < periodUnits.length; i++) {
        this.transformations[i] = parse(referencePosition.get(periodUnits[i]));
      }
    }

    private Object parse(String transformation) {
      Pattern shiftPattern = Pattern.compile("[a-zA-Z]+([-+])(\\d)");
      Pattern constantPattern = Pattern.compile("[a-zA-Z]+");
      Matcher m;
      if ((m = shiftPattern.matcher(transformation)).matches()) {
        String signum = m.group(1);
        String shift = m.group(2);
        return (signum.equals("-") ? -1 : 1) * Integer.valueOf(shift);
      } else if (constantPattern.matcher(transformation).matches()) {
        return null; // nothing to do
      } else {
        throw new RuntimeException("Unsupported transformation: " + transformation);
      }
    }

    public void execute(Object[] position) {
      if (this.period instanceof Period.QuarterFromMonthYear || this.period instanceof Period.QuarterFromDate) {
        // YEAR, QUARTER
        int year = (int) position[0];
        if (this.referencePosition.containsKey(BinaryOperationMeasure.PeriodUnit.YEAR)) {
          if (this.transformations[0] != null) {
            position[0] = year + (int) this.transformations[0];
          }
        }
        if (this.referencePosition.containsKey(BinaryOperationMeasure.PeriodUnit.QUARTER)) {
          int quarter = (int) position[1];
          if (this.transformations[1] != null) {
            LocalDate d = LocalDate.of((Integer) position[0], quarter * 3, 1);
            LocalDate newd = d.plusMonths(((int) this.transformations[1]) * 3);
            position[1] = (int) IsoFields.QUARTER_OF_YEAR.getFrom(newd);
            position[0] = newd.getYear(); // year might have changed
          }
        }
      } else if (this.period instanceof Period.YearFromDate || this.period instanceof Period.Year) {
        // YEAR
        int year = (int) position[0];
        if (this.referencePosition.containsKey(BinaryOperationMeasure.PeriodUnit.YEAR)) {
          if (this.transformations[0] != null) {
            position[0] = year + (int) this.transformations[0];
          }
        }
      } else {
        throw new RuntimeException(this.period + " not supported yet");
      }
    }

    private static BinaryOperationMeasure.PeriodUnit[] getPeriodUnits(Period period) {
      if (period instanceof Period.QuarterFromMonthYear) {
        return new BinaryOperationMeasure.PeriodUnit[]{BinaryOperationMeasure.PeriodUnit.YEAR, BinaryOperationMeasure.PeriodUnit.QUARTER};
      } else if (period instanceof Period.QuarterFromDate) {
        return new BinaryOperationMeasure.PeriodUnit[]{BinaryOperationMeasure.PeriodUnit.YEAR, BinaryOperationMeasure.PeriodUnit.QUARTER};
      } else if (period instanceof Period.YearFromDate || period instanceof Period.Year) {
        return new BinaryOperationMeasure.PeriodUnit[]{BinaryOperationMeasure.PeriodUnit.YEAR};
      } else {
        throw new RuntimeException(period + " not supported yet");
      }
    }

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
