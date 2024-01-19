package io.squashql.query;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class AliasedField implements NamedField {

  public String alias;

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public NamedField as(String alias) {
    return new AliasedField(alias); // does not make sense...
  }

  @Override
  public String alias() {
    return this.alias;
  }
}
