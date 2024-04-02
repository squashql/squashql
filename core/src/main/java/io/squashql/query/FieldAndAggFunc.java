package io.squashql.query;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FieldAndAggFunc {
  public Field field;
  public String aggFunc;
}
