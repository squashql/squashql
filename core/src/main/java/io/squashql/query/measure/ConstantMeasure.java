package io.squashql.query.measure;

import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public abstract class ConstantMeasure<T> implements Measure {

  public T value;
  @With
  public String expression;

  public ConstantMeasure(@NonNull T value) {
    this.value = value;
  }

  public T getValue() {
    return this.value;
  }

  @Override
  public String alias() {
    return "constant(" + this.value + ")";
  }

  @Override
  public String expression() {
    return this.expression;
  }
}
