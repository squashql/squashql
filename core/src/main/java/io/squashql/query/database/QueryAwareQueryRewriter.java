package io.squashql.query.database;

import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;

/**
 * A {@link QueryRewriter} whose logic depends on the query being executed. See {@link #getFieldFullName(TableTypedField)}.
 */
public class QueryAwareQueryRewriter implements QueryRewriter {

  private final QueryRewriter underlying;

  private final DatabaseQuery query;

  public QueryAwareQueryRewriter(QueryRewriter underlying, DatabaseQuery query) {
    this.underlying = underlying;
    this.query = query;
  }

  @Override
  public String getFieldFullName(TableTypedField f) {
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
  public String functionExpression(FunctionTypedField ftf) {
    return this.underlying.functionExpression(ftf);
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
  public String measureAlias(String alias) {
    return this.underlying.measureAlias(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    return this.underlying.usePartialRollupSyntax();
  }

  @Override
  public String escapeSingleQuote(String s) {
    return this.underlying.escapeSingleQuote(s);
  }
}
