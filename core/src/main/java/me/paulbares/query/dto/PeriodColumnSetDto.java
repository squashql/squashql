package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.store.Field;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class PeriodColumnSetDto {

  public static List<String> getColumnsForPrefetching(Period period) {
    if (period instanceof Period.Quarter q) {
      return List.of(q.year(), q.quarter());
    } else if (period instanceof Period.Year y) {
      return List.of(y.year());
    } else if (period instanceof Period.Month m) {
      return List.of(m.year(), m.month());
    } else if (period instanceof Period.Semester s) {
      return List.of(s.year(), s.semester());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  public static List<Field> getNewColumns(Period period) {
    if (period instanceof Period.Quarter q) {
      return List.of(new Field(q.year(), int.class), new Field(q.quarter(), int.class));
    } else if (period instanceof Period.Year y) {
      return List.of(new Field(y.year(), int.class));
    } else if (period instanceof Period.Month m) {
      return List.of(new Field(m.year(), int.class), new Field(m.month(), int.class));
    } else if (period instanceof Period.Semester s) {
      return List.of(new Field(s.year(), int.class), new Field(s.semester(), int.class));
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }
}
