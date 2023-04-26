package io.squashql.query.database;

import io.squashql.query.dto.VirtualTableDto;
import io.squashql.store.Field;

/**
 * A {@link QueryRewriter} whose logic depends on the query being executed. See {@link #getFieldFullName(Field)}.
 */
public class QueryAwareQueryRewriter implements QueryRewriter {

  private final QueryRewriter underlying;

  private final DatabaseQuery query;

  public QueryAwareQueryRewriter(QueryRewriter underlying, DatabaseQuery query) {
    this.underlying = underlying;
    this.query = query;
  }

  @Override
  public String getFieldFullName(Field f) {
    VirtualTableDto vt = this.query.virtualTableDto;
    if (vt != null
            && vt.name.equals(f.store())
            && vt.fields.contains(f.name())) {
      return SqlUtils.getFieldFullName(cteName(f.store()), fieldName(f.name()));
    } else {
      return this.underlying.getFieldFullName(f);
    }
  }

  @Override
  public String fieldName(String field) {
    return this.underlying.fieldName(field);
  }

  @Override
  public String tableName(String table) {
    return this.underlying.tableName(table);
  }

  @Override
  public String cteName(String cteName) {
    return this.underlying.cteName(cteName);
  }

  @Override
  public String select(Field f) {
    return getFieldFullName(f);
  }

  @Override
  public String rollup(Field f) {
    return getFieldFullName(f);
  }

  @Override
  public String measureAlias(String alias) {
    return this.underlying.measureAlias(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    return this.underlying.usePartialRollupSyntax();
  }

  @Override
  public boolean useGroupingFunction() {
    return this.underlying.useGroupingFunction();
  }
}
