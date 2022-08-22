package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class BinaryOperationMeasure implements Measure {

  public String alias;
  public String expression;
  public BinaryOperator operator;
  public Measure leftOperand;
  public Measure rightOperand;

  public BinaryOperationMeasure(String alias,
                                BinaryOperator binaryOperator,
                                Measure leftOperand,
                                Measure rightOperand) {
    this.alias = alias;
    this.operator = binaryOperator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
    this.expression = MeasureUtils.createExpression(this);
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }
}
