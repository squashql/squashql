package me.paulbares.query;

import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodColumnSetDto;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class PeriodComparisonExecutor extends AComparisonExecutor {

  final PeriodColumnSetDto cSet;

  public PeriodComparisonExecutor(PeriodColumnSetDto cSet) {
    this.cSet = cSet;
  }

  @Override
  public ColumnSet getColumnSet() {
    return this.cSet;
  }

  @Override
  protected Predicate<Object[]> createShiftProcedure(ComparisonMeasure cm, ObjectIntMap<String> indexByColumn) {
    Map<PeriodUnit, String> referencePosition = new HashMap<>();
    Map<String, PeriodUnit> mapping = this.cSet.mapping();
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    for (Map.Entry<String, String> entry : cm.referencePosition.entrySet()) {
      PeriodUnit pu = mapping.get(entry.getKey());
      referencePosition.put(pu, entry.getValue());
      indexByPeriodUnit.put(pu, indexByColumn.getIfAbsent(entry.getKey(), -1));
    }
    return new ShiftProcedure(this.cSet.period, referencePosition, indexByPeriodUnit);
  }

  static class ShiftProcedure implements Predicate<Object[]> {

    final Period period;
    final Map<PeriodUnit, String> referencePosition;
    final ObjectIntMap<PeriodUnit> indexByPeriodUnit;
    final Map<PeriodUnit, Object> transformationByPeriodUnit;

    ShiftProcedure(Period period,
                   Map<PeriodUnit, String> referencePosition,
                   ObjectIntMap<PeriodUnit> indexByPeriodUnit) {
      this.period = period;
      this.referencePosition = referencePosition;
      this.indexByPeriodUnit = indexByPeriodUnit;
      PeriodUnit[] periodUnits = getPeriodUnits(period);
      this.transformationByPeriodUnit = new HashMap<>();
      for (int i = 0; i < periodUnits.length; i++) {
        Object parse = parse(referencePosition.get(periodUnits[i]));
        if (parse != null) {
          this.transformationByPeriodUnit.put(periodUnits[i], parse);
        }
      }
    }

    @Override
    public boolean test(Object[] row) {
      int yearIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.YEAR, -1);
      int quarterIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.QUARTER, -1);
      Object yearTransformation = this.transformationByPeriodUnit.get(PeriodUnit.YEAR);
      Object quarterTransformation = this.transformationByPeriodUnit.get(PeriodUnit.QUARTER);
      if (this.period instanceof Period.Quarter) {
        // YEAR, QUARTER
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            row[yearIndex] = year + (int) yearTransformation;
          }
        }
        if (this.referencePosition.containsKey(PeriodUnit.QUARTER)) {
          int quarter = readAsLong(row[quarterIndex]);
          if (quarterTransformation != null) {
            LocalDate d = LocalDate.of((Integer) row[yearIndex], quarter * 3, 1);
            LocalDate newDate = d.plusMonths(((int) quarterTransformation) * 3);
            row[quarterIndex] = (int) IsoFields.QUARTER_OF_YEAR.getFrom(newDate);
            row[yearIndex] = newDate.getYear(); // year might have changed
          }
        }
      } else if (this.period instanceof Period.Year) {
        // YEAR
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            row[yearIndex] = year + (int) yearTransformation;
          }
        }
      } else {
        throw new RuntimeException(this.period + " not supported yet");
      }
      return true;
    }

    private static int readAsLong(Object o) {
      return (int) ((Number) o).longValue(); // with some database, year could be Long object.
    }

    private static PeriodUnit[] getPeriodUnits(Period period) {
      if (period instanceof Period.Quarter) {
        return new PeriodUnit[]{PeriodUnit.YEAR, PeriodUnit.QUARTER};
      } else if (period instanceof Period.Year) {
        return new PeriodUnit[]{PeriodUnit.YEAR};
      } else {
        throw new RuntimeException(period + " not supported yet");
      }
    }
  }
}
