package io.squashql.query.compiled;

import io.squashql.query.TableField;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.*;
import io.squashql.type.TypedField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public record CompiledCriteria(TypedField field, TypedField fieldOther, CompiledMeasure measure, ConditionDto condition, ConditionType conditionType, List<CompiledCriteria> children){

  public String sqlExpression(QueryRewriter queryRewriter) {
    if (this.field != null && this.condition != null) {
      return toSql(this.field, this.condition, queryRewriter);
    } else if (this.measure != null && this.condition != null) {
      return toSql(new TableField(this.measure.alias()), this.condition, queryRewriter);
    } else if (this.field != null && this.fieldOther != null && this.conditionType != null) {
      String left = this.field.sqlExpression(queryRewriter);
      String right = this.fieldOther.sqlExpression(queryRewriter);
      return String.join(" ", left, this.conditionType.sqlInfix, right);
    } else if (!this.children.isEmpty()) {
      String sep = switch (this.conditionType) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Unexpected value: " + this.conditionType);
      };
      Iterator<CompiledCriteria> iterator = this.children.iterator();
      List<String> conditions = new ArrayList<>();
      while (iterator.hasNext()) {
        String c = iterator.next().sqlExpression(queryRewriter);
        if (c != null) {
          conditions.add(c);
        }
      }
      return conditions.isEmpty() ? null : ("(" + String.join(sep, conditions) + ")");
    } else {
      return null;
    }
  }

  public static String toSql(TypedField field, ConditionDto dto, QueryRewriter queryRewriter) {
    String expression = field.sqlExpression(queryRewriter);
    if (dto instanceof SingleValueConditionDto || dto instanceof InConditionDto) {
      Function<Object, String> sqlMapper = field instanceof TableField ? getQuoteFn(field) : String::valueOf; // FIXME dirty workaround
      return switch (dto.type()) {
        case IN -> expression + " " + dto.type().sqlInfix + " (" +
                ((InConditionDto) dto).values
                        .stream()
                        .map(sqlMapper)
                        .collect(Collectors.joining(", ")) + ")";
        case EQ, NEQ, LT, LE, GT, GE, LIKE ->
                expression + " " + dto.type().sqlInfix + " " + sqlMapper.apply(((SingleValueConditionDto) dto).value);
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else if (dto instanceof LogicalConditionDto logical) {
      String first = toSql(field, logical.one, queryRewriter);
      String second = toSql(field, logical.two, queryRewriter);
      String typeString = switch (dto.type()) {
        case AND, OR -> " " + ((LogicalConditionDto) dto).type.sqlInfix + " ";
        default -> throw new IllegalStateException("Incorrect type " + logical.type);
      };
      return "(" + first + typeString + second + ")";
    } else if (dto instanceof ConstantConditionDto cc) {
      return switch (cc.type()) {
        case NULL, NOT_NULL -> expression + " " + cc.type.sqlInfix;
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else {
      throw new RuntimeException("Not supported condition " + dto);
    }
  }

}
