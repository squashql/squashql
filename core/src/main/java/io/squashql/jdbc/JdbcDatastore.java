package io.squashql.jdbc;

import io.squashql.store.Datastore;

import java.sql.Connection;

public interface JdbcDatastore extends Datastore {

  Connection getConnection();
}
