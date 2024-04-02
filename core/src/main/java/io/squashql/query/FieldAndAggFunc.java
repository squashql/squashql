package io.squashql.query;

import io.squashql.query.field.Field;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FieldAndAggFunc {
  public Field field;
  public String aggFunc;
}
