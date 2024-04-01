package io.squashql.store;

import java.util.Map;

public interface Datastore {

  Map<String, Store> storeByName();
}
