package io.squashql.query.field;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class AliasedField implements Field {

  public String alias;

  @Override
  public Field as(String alias) {
    return new AliasedField(alias); // does not make sense...
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
