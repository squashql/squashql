package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor // For Jackson
public class LongConstantMeasure extends ConstantMeasure<Long> {

  public LongConstantMeasure(@NonNull Long value) {
    super(value);
  }
}
