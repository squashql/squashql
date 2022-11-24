package me.paulbares.store;

import java.util.Map;

public interface Datastore {

  Map<String, Store> storesByName();
}
