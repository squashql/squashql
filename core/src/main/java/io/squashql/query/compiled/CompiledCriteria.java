package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlTranslator;
import io.squashql.query.dto.*;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public record CompiledCriteria(ConditionDto condition, ConditionType conditionType, TypedField field,
                               TypedField fieldOther, CompiledMeasure measure, List<CompiledCriteria> children) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    if (this.field != null && condition() != null) {
      return toSql(this.field, condition(), queryRewriter);
    } else if (this.measure != null && condition() != null) {
      if (queryRewriter.useAliasInHavingClause()) {
        return toSql(new TableTypedField(null, this.measure.alias(), Number.class), condition(), queryRewriter);
      } else {
        // Trick to repeat the expression in the having clause
        String expression = this.measure.sqlExpression(queryRewriter, false);
        return toSql(new TableTypedField(null, expression, Number.class), condition(), new QueryRewriter() {
        });
      }
    } else if (this.field != null && this.fieldOther != null && conditionType() != null) {
      String left = this.field.sqlExpression(queryRewriter);
      String right = this.fieldOther.sqlExpression(queryRewriter);
      return String.join(" ", left, conditionType().sqlInfix, right);
    } else if (!this.children.isEmpty()) {
      String sep = switch (conditionType()) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Unexpected value: " + conditionType());
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
    String expression = field.sqlExpression(queryRewriter); // FIXME should be queryRewriter.where(field) because some db supports using the alias, others not.

    if (dto instanceof SingleValueConditionDto svc) {
      Object value = svc.value;
      Function<Object, String> sqlMapper = SqlTranslator.getQuoteFn(field.type(), value.getClass(), queryRewriter);
      return switch (dto.type()) {
        case EQ, NEQ, LT, LE, GT, GE, LIKE -> expression + " " + dto.type().sqlInfix + " " + sqlMapper.apply(value);
        case ARRAY_CONTAINS -> queryRewriter.arrayContains(field, sqlMapper.apply(value));
        default -> throw new IllegalStateException("Unexpected value: " + dto.type());
      };
    } else if (dto instanceof InConditionDto ic) {
      Function<Object, String> sqlMapper = SqlTranslator.getQuoteFn(field.type(), ic.values.iterator().next().getClass(), queryRewriter);
      String sep = ic.invert ? " not " : " ";
      return expression + sep + dto.type().sqlInfix + " (" +
              ic.values
                      .stream()
                      .map(sqlMapper)
                      .collect(Collectors.joining(", ")) + ")";
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

  public static CompiledCriteria deepCopy(CompiledCriteria criteria) {
    if (criteria.children == null || criteria.children.isEmpty()) {
      return new CompiledCriteria(
              criteria.condition(),
              criteria.conditionType(),
              criteria.field,
              criteria.fieldOther,
              criteria.measure,
              Collections.emptyList());
    } else {
      List<CompiledCriteria> list = new ArrayList<>(criteria.children.size());
      for (CompiledCriteria dto : criteria.children) {
        CompiledCriteria copy = deepCopy(dto);
        list.add(copy);
      }
      return new CompiledCriteria(criteria.condition(), criteria.conditionType(), null, null, null, list);
    }
  }

}
