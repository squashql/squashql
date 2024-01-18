package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

import java.util.List;

/**
 * FIXME to rename CompiledTable latter on
 */
public interface CompiledTable {

  List<CompiledJoin> joins();

  String sqlExpression(QueryRewriter queryRewriter);
}
