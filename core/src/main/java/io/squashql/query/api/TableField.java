package io.squashql.query.api;

import io.squashql.query.CountMeasure;
import io.squashql.query.MeasureUtils;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class TableField implements Field {

  public String name;

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    if (CountMeasure.FIELD_NAME.equals(this.name)) {
      return CountMeasure.FIELD_NAME;
    } else {
      Function<String, TypedField> fp = MeasureUtils.withFallback(fieldProvider, String.class);
      TypedField f = fp.apply(this.name);
      return queryRewriter.getFieldFullName(f);
    }
  }
}
