package io.squashql.query.dto;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.parameter.Parameter;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.IOException;
import java.util.*;

import static io.squashql.query.dto.ConditionType.AND;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryDto {

  public TableDto table;

  public QueryDto subQuery;

  public List<VirtualTableDto> virtualTableDtos = new ArrayList<>();

  public List<Field> columns = new ArrayList<>();

  public List<Field> rollupColumns = new ArrayList<>();

  /**
   * Internal ONLY! This field is not supposed to be set by the external API and is incompatible with {@link #rollupColumns}
   */
  public List<List<Field>> groupingSets = new ArrayList<>();

  public Map<ColumnSetKey, ColumnSet> columnSets = new LinkedHashMap<>();

  public List<Measure> measures = new ArrayList<>();

  public CriteriaDto whereCriteriaDto = null;

  public CriteriaDto havingCriteriaDto = null;

  public Map<Field, OrderDto> orders = new LinkedHashMap<>();
  /*
   * - Order by measure
   * - tests
   *  # multiple switch case
   *  # mix switch case and simple order at different position
   *  # test sql translator
   *  # test order by on squashql computed measure
   */
  public Map<String, Parameter> parameters = new HashMap<>();

  public int limit = -1;

  public QueryDto withColumn(Field column) {
    this.columns.add(column);
    return this;
  }

  public QueryDto withRollup(Field column) {
    this.rollupColumns.add(column);
    return this;
  }

  public QueryDto withColumnSet(ColumnSetKey columnSetKey, ColumnSet columnSet) {
    this.columnSets.put(columnSetKey, columnSet);
    return this;
  }

  public QueryDto withMeasure(Measure m) {
    this.measures.add(m);
    return this;
  }

  public QueryDto withParameter(String key, Parameter value) {
    this.parameters.put(key, value);
    return this;
  }

  public QueryDto table(TableDto table) {
    this.table = table;
    return this;
  }

  public QueryDto table(QueryDto subQuery) {
    this.subQuery = subQuery;
    return this;
  }

  public QueryDto table(String tableName) {
    table(new TableDto(tableName));
    return this;
  }

  public QueryDto withCondition(Field field, ConditionDto conditionDto) {
    if (this.whereCriteriaDto == null) {
      this.whereCriteriaDto = new CriteriaDto(AND, new ArrayList<>());
    }
    this.whereCriteriaDto.children.add(new CriteriaDto(field, conditionDto));
    return this;
  }

  public QueryDto withWhereCriteria(CriteriaDto criteriaDto) {
    this.whereCriteriaDto = criteriaDto;
    return this;
  }

  public QueryDto withHavingCriteria(CriteriaDto criteriaDto) {
    this.havingCriteriaDto = criteriaDto;
    return this;
  }

  public QueryDto orderBy(Field column, OrderKeywordDto orderKeywordDto) {
    this.orders.put(column, new SimpleOrderDto(orderKeywordDto));
    return this;
  }

  public QueryDto orderBy(Field column, List<?> firstElements) {
    this.orders.put(column, new ExplicitOrderDto(firstElements));
    return this;
  }

  public QueryDto withLimit(int limit) {
    this.limit = limit;
    return this;
  }

  public String json() {
    return JacksonUtil.serialize(this);
  }

  public static class KeyFieldSerializer extends JsonSerializer<Field> {
    @Override
    public void serialize(Field field, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeFieldName(JacksonUtil.serialize(field));
    }
  }

  public static class KeyFieldDeserializer extends KeyDeserializer {

    @Override
    public Field deserializeKey(
            String key,
            DeserializationContext context) {
      return JacksonUtil.deserialize(key, Field.class);
    }
  }
}
