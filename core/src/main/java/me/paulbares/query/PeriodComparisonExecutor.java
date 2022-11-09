package me.paulbares.query;

import me.paulbares.query.dto.Period;
import me.paulbares.store.Field;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;

import static me.paulbares.query.PeriodUnit.*;

public class PeriodComparisonExecutor extends AComparisonExecutor {

  final ComparisonMeasureReferencePosition cmrp;

  public PeriodComparisonExecutor(ComparisonMeasureReferencePosition cmrp) {
    this.cmrp = cmrp;
  }

  public Map<String, PeriodUnit> mapping(Period period) {
    if (period instanceof Period.Quarter q) {
      return Map.of(q.quarter(), QUARTER, q.year(), YEAR);
    } else if (period instanceof Period.Year y) {
      return Map.of(y.year(), YEAR);
    } else if (period instanceof Period.Month m) {
      return Map.of(m.month(), MONTH, m.year(), YEAR);
    } else if (period instanceof Period.Semester s) {
      return Map.of(s.semester(), SEMESTER, s.year(), YEAR);
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  @Override
  protected BiPredicate<Object[], Field[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn) {
    Map<PeriodUnit, String> referencePosition = new HashMap<>();
    Period period = this.cmrp.period;
    Map<String, PeriodUnit> mapping = mapping(period);
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    for (Map.Entry<String, String> entry : cm.referencePosition.entrySet()) {
      PeriodUnit pu = mapping.get(entry.getKey());
      referencePosition.put(pu, entry.getValue());
      indexByPeriodUnit.put(pu, indexByColumn.getIfAbsent(entry.getKey(), -1));
    }
    for (Map.Entry<String, PeriodUnit> entry : mapping.entrySet()) {
      PeriodUnit pu = mapping.get(entry.getKey());
      referencePosition.putIfAbsent(pu, "c"); // constant for missing ref.
      indexByPeriodUnit.put(pu, indexByColumn.getIfAbsent(entry.getKey(), -1));
    }
    return new ShiftProcedure(period, referencePosition, indexByPeriodUnit);
  }

  static class ShiftProcedure implements BiPredicate<Object[], Field[]> {

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
    public boolean test(Object[] row, Field[] fields) {
      int unknown = -1;
      int yearIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.YEAR, unknown);
      int semesterIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.SEMESTER, unknown);
      int quarterIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.QUARTER, unknown);
      int monthIndex = this.indexByPeriodUnit.getIfAbsent(PeriodUnit.MONTH, unknown);
      Object yearTransformation = this.transformationByPeriodUnit.get(PeriodUnit.YEAR);
      Object semesterTransformation = this.transformationByPeriodUnit.get(PeriodUnit.SEMESTER);
      Object quarterTransformation = this.transformationByPeriodUnit.get(PeriodUnit.QUARTER);
      Object monthTransformation = this.transformationByPeriodUnit.get(PeriodUnit.MONTH);
      if (this.period instanceof Period.Quarter) {
        // YEAR, QUARTER
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            write(row, yearIndex, fields[yearIndex], year + (int) yearTransformation);
          }
        }
        if (this.referencePosition.containsKey(PeriodUnit.QUARTER)) {
          int quarter = readAsLong(row[quarterIndex]);
          if (quarterTransformation != null) {
            LocalDate d = LocalDate.of(readAsLong(row[yearIndex]), quarter * 3, 1);
            LocalDate newDate = d.plusMonths(((int) quarterTransformation) * 3);
            write(row, quarterIndex, fields[quarterIndex], (int) IsoFields.QUARTER_OF_YEAR.getFrom(newDate));
            write(row, yearIndex, fields[yearIndex], newDate.getYear());// year might have changed
          }
        }
      } else if (this.period instanceof Period.Year) {
        // YEAR
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            write(row, yearIndex, fields[yearIndex], year + (int) yearTransformation);
          }
        }
      } else if (this.period instanceof Period.Month) {
        // YEAR, MONTH
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            write(row, yearIndex, fields[yearIndex], year + (int) yearTransformation);
          }
        }
        if (this.referencePosition.containsKey(PeriodUnit.MONTH)) {
          int month = readAsLong(row[monthIndex]);
          if (monthTransformation != null) {
            LocalDate newDate = LocalDate.of(readAsLong(row[yearIndex]), month, 1)
                    .plusMonths((int) monthTransformation);
            write(row, monthIndex, fields[monthIndex], newDate.getMonthValue());
            write(row, yearIndex, fields[yearIndex], newDate.getYear()); // year might have changed
          }
        }
      } else if (this.period instanceof Period.Semester) {
        // YEAR, SEMESTER
        int year = readAsLong(row[yearIndex]);
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            write(row, yearIndex, fields[yearIndex], year + (int) yearTransformation);
          }
        }
        if (this.referencePosition.containsKey(PeriodUnit.SEMESTER)) {
          int semester = readAsLong(row[semesterIndex]);
          if (semesterTransformation != null) {
            LocalDate d = LocalDate.of(readAsLong(row[yearIndex]), semester * 6, 1);
            LocalDate newDate = d.plusMonths(((int) semesterTransformation) * 6);
            write(row, semesterIndex, fields[semesterIndex], newDate.getMonthValue() / 6);
            write(row, yearIndex, fields[yearIndex], newDate.getYear()); // year might have changed
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

    private static void write(Object[] rowIndex, int index, Field field, int number) {
      if (field.type() == Long.class || field.type() == long.class) {
        rowIndex[index] = (long) number;
      } else if (field.type() == Integer.class || field.type() == int.class) {
        rowIndex[index] = number;
      } else {
        throw new IllegalArgumentException("Unsupported type " + field.type());
      }
    }

    private static PeriodUnit[] getPeriodUnits(Period period) {
      if (period instanceof Period.Quarter) {
        return new PeriodUnit[]{PeriodUnit.YEAR, PeriodUnit.QUARTER};
      } else if (period instanceof Period.Year) {
        return new PeriodUnit[]{PeriodUnit.YEAR};
      } else if (period instanceof Period.Month) {
        return new PeriodUnit[]{PeriodUnit.YEAR, PeriodUnit.MONTH};
      } else if (period instanceof Period.Semester) {
        return new PeriodUnit[]{PeriodUnit.YEAR, PeriodUnit.SEMESTER};
      } else {
        throw new RuntimeException(period + " not supported yet");
      }
    }
  }
}
