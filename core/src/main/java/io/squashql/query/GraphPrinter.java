package io.squashql.query;

import io.squashql.type.TypedField;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntSupplier;

public class GraphPrinter {

  public static <N> void print(DependencyGraph<N> graph) {
    StringBuilder sb = new StringBuilder();

    Set<DependencyGraph.NodeWithId<N>> roots = new HashSet<>();
    for (DependencyGraph.NodeWithId<N> node : graph.nodes()) {
      if (graph.inDegree(node) == 0) {
        roots.add(node);
      }
    }

    IntSupplier id = new AtomicInteger()::getAndIncrement;
    Map<QueryExecutor.QueryScope, Integer> idByScope = new HashMap<>();
    Function<QueryExecutor.QueryScope, Integer> idProvider = scope -> idByScope.computeIfAbsent(scope, k -> id.getAsInt());
    Set<N> alreadyPrinted = new HashSet<>();
    for (DependencyGraph.NodeWithId<N> root : roots) {
      appendNode(sb, root, alreadyPrinted, idProvider);
      sb.append(System.lineSeparator());
      executeRecursively(sb, graph, root, alreadyPrinted, idProvider, 1);
    }

    sb.append(System.lineSeparator());
    sb.append("Scopes:").append(System.lineSeparator());
    for (Map.Entry<QueryExecutor.QueryScope, Integer> e : idByScope.entrySet()) {
      sb.append("#").append(e.getValue()).append(": ").append(printQueryPlanNodeKey(e.getKey())).append(System.lineSeparator());
    }

    System.out.println(sb);
  }

  private static <N> void executeRecursively(StringBuilder sb, DependencyGraph graph, DependencyGraph.NodeWithId<N> node, Set<N> alreadyPrinted, Function<QueryExecutor.QueryScope, Integer> idProvider, int level) {
    Set<DependencyGraph.NodeWithId<N>> successors = graph.successors(node);
    for (DependencyGraph.NodeWithId<N> successor : successors) {
      for (int i = 0; i < level; i++) {
        sb.append("\t");
      }
      appendNode(sb, successor, alreadyPrinted, idProvider);
      sb.append(System.lineSeparator());
      executeRecursively(sb, graph, successor, alreadyPrinted, idProvider, level + 1);
    }
  }

  private static <N> void appendNode(StringBuilder sb, DependencyGraph.NodeWithId<N> node, Set<N> alreadyPrinted, Function<QueryExecutor.QueryScope, Integer> idProvider) {
    sb.append("#").append(node.id);
    if (alreadyPrinted.add(node.node)) {
      sb.append(", n=").append(printQueryPlanNodeKey(idProvider, node.node));
    }
  }

  private static String printQueryPlanNodeKey(Function<QueryExecutor.QueryScope, Integer> idProvider, Object o) {
    if (o instanceof QueryExecutor.QueryPlanNodeKey key) {
      StringBuilder sb = new StringBuilder();
      sb.append("scope=").append("#").append(idProvider.apply(key.queryScope()));
      sb.append(", measure=[").append(key.measure().alias()).append("]; [").append(key.measure()).append("]");
      return sb.toString();
    } else {
      return String.valueOf(o);
    }
  }

  private static String printQueryPlanNodeKey(QueryExecutor.QueryScope scope) {
    StringBuilder sb = new StringBuilder();
    appendIfNotNullOrNotEmpty(sb, null, scope.table());
    appendIfNotNullOrNotEmpty(sb, "columns=", scope.columns().stream().map(TypedField::toString).toList());
    appendIfNotNullOrNotEmpty(sb, null, scope.whereCriteria());
    appendIfNotNullOrNotEmpty(sb, "rollup=", scope.rollupColumns().stream().map(TypedField::toString).toList());
    return sb.toString();
  }

  private static void appendIfNotNullOrNotEmpty(StringBuilder sb, String prefix, Object o) {
    if (o instanceof Collection collection) {
      if (collection.isEmpty()) {
        return;
      }
      sb.append(prefix == null ? "" : prefix).append(o).append(", ");
    } else if (o != null) {
      sb.append(prefix == null ? "" : prefix).append(o).append(", ");
    }
  }
}
