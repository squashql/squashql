package io.squashql.query.measure;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.squashql.jackson.ParametrizedMeasureDeserializer;
import io.squashql.jackson.ParametrizedMeasureSerializer;
import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
@JsonDeserialize(using = ParametrizedMeasureDeserializer.class)
@JsonSerialize(using = ParametrizedMeasureSerializer.class)
public class ParametrizedMeasure implements Measure {

  public String alias;

  public String key;

  public Map<String, Object> parameters;

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.key;
  }

  @Override
  public Measure withExpression(String __) {
    return this;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
