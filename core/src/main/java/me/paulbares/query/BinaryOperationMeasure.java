package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
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

  public BinaryOperationMeasure(@NonNull String alias,
                                @NonNull BinaryOperator binaryOperator,
                                @NonNull Measure leftOperand,
                                @NonNull Measure rightOperand) {
    this.alias = alias;
    this.operator = binaryOperator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
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

  @Override
  public void setExpression(String expression) {
    this.expression = expression;
  }
}
