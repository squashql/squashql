package io.squashql.query.database;

public class SparkQueryRewriter extends DefaultQueryRewriter {

  @Override
  public String escapeSingleQuote(String s) {
    return SqlUtils.escapeSingleQuote(s, "\\'");
  }
}
