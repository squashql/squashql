package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor // For Jackson
public class DoubleConstantMeasure extends ConstantMeasure<Double> {

  public DoubleConstantMeasure(@NonNull Double value) {
    super(value);
  }
}