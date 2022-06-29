package me.paulbares.query;

import me.paulbares.query.BinaryOperationMeasure.PeriodUnit;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodColumnSetDto;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PeriodComparisonExecutor {

  public static List<Object> compare(
          BinaryOperationMeasure bom,
          PeriodColumnSetDto columnSet,
          Table intermediateResult) {
    Map<PeriodUnit, String> referencePosition = new HashMap<>();
    Map<PeriodUnit, String> mapping = columnSet.mapping();
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    for (Map.Entry<String, String> entry : bom.referencePosition.entrySet()) { // quarter, year
      PeriodUnit pu = PeriodUnit.valueOf(entry.getKey());
      referencePosition.put(pu, entry.getValue());
      indexByPeriodUnit.put(pu, intermediateResult.columnIndex(mapping.get(pu)));
    }
    ShiftProcedure procedure = new ShiftProcedure(columnSet.period, referencePosition, indexByPeriodUnit);
    Object[] buffer = new Object[intermediateResult.columnIndices().length];
    List<Object> result = new ArrayList<>((int) intermediateResult.count());
    int[] rowIndex = new int[1];
    List<Object> aggregateValues = intermediateResult.getAggregateValues(bom.measure);
    intermediateResult.forEach(row -> {
      int i = 0;
      for (int columnIndex : intermediateResult.columnIndices()) {
        buffer[i++] = row.get(columnIndex);
      }
      procedure.execute(buffer);
      int position = intermediateResult.pointDictionary().getPosition(buffer);
      if (position != -1) {
        Object currentValue = aggregateValues.get(rowIndex[0]);
        Object referenceValue = aggregateValues.get(position);
        Object diff = BinaryOperations.compare(bom.method, currentValue, referenceValue, intermediateResult.getField(bom.measure).type());
        result.add(diff);
      } else {
        result.add(null); // nothing to compare with
      }
      rowIndex[0]++;
    });

    return result;
  }

  static class ShiftProcedure {

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
        this.transformationByPeriodUnit.put(periodUnits[i], parse(referencePosition.get(periodUnits[i])));
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

    public void execute(Object[] row) {
      int yearIndex = this.indexByPeriodUnit.get(PeriodUnit.YEAR);
      int quarterIndex = this.indexByPeriodUnit.get(PeriodUnit.QUARTER);
      Object yearTransformation = this.transformationByPeriodUnit.get(PeriodUnit.YEAR);
      Object quarterTransformation = this.transformationByPeriodUnit.get(PeriodUnit.QUARTER);
      if (this.period instanceof Period.Quarter) {
        // YEAR, QUARTER
        int year = (int) row[yearIndex];
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            row[yearIndex] = year + (int) yearTransformation;
          }
        }
        if (this.referencePosition.containsKey(PeriodUnit.QUARTER)) {
          int quarter = (int) row[quarterIndex];
          if (quarterTransformation != null) {
            LocalDate d = LocalDate.of((Integer) row[yearIndex], quarter * 3, 1);
            LocalDate newDate = d.plusMonths(((int) quarterTransformation) * 3);
            row[quarterIndex] = (int) IsoFields.QUARTER_OF_YEAR.getFrom(newDate);
            row[yearIndex] = newDate.getYear(); // year might have changed
          }
        }
      } else if (this.period instanceof Period.Year) {
        // YEAR
        int year = (int) row[yearIndex];
        if (this.referencePosition.containsKey(PeriodUnit.YEAR)) {
          if (yearTransformation != null) {
            row[yearIndex] = year + (int) yearTransformation;
          }
        }
      } else {
        throw new RuntimeException(this.period + " not supported yet");
      }
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
