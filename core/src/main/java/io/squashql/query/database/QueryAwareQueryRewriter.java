package io.squashql.query.database;

import io.squashql.query.dto.VirtualTableDto;
import io.squashql.store.TypedField;

/**
 * A {@link QueryRewriter} whose logic depends on the query being executed. See {@link #getFieldFullName(TypedField)}.
 */
public class QueryAwareQueryRewriter implements QueryRewriter {

  private final QueryRewriter underlying;

  private final DatabaseQuery query;

  public QueryAwareQueryRewriter(QueryRewriter underlying, DatabaseQuery query) {
    this.underlying = underlying;
    this.query = query;
  }

  @Override
  public String getFieldFullName(TypedField f) {
    VirtualTableDto vt = this.query.virtualTableDto;
    if (vt != null
            && vt.name.equals(f.store())
            && vt.fields.contains(f.name())) {
      return SqlUtils.getFieldFullName(cteName(f.store()), fieldName(f.name()));
      // todo-181 should we care about this case ?
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
  public String rollup(TypedField f) {
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
