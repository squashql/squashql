package io.squashql.query;


import io.squashql.query.DependencyGraph.NodeWithId;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ExecutionPlan<N, Context> {

  private final BiConsumer<N, Context> consumer;
  private final DependencyGraph<N> graph;
  private final Set<N> processed = new HashSet<>();
  private final Set<NodeWithId<N>> roots = new HashSet<>();
  private final Set<NodeWithId<N>> leaves = new HashSet<>();

  public ExecutionPlan(DependencyGraph<N> graph, BiConsumer<N, Context> consumer) {
    this.graph = graph;
    this.consumer = consumer;

    Set<NodeWithId<N>> nodes = this.graph.nodes();
    for (NodeWithId<N> node : nodes) {
      if (this.graph.outDegree(node) == 0) {
        this.leaves.add(node);
      }

      if (this.graph.inDegree(node) == 0) {
        this.roots.add(node);
      }
    }
  }

  public void execute(Context context) {
    for (NodeWithId<N> node : this.roots) {
      executeRecursively(node, context);
    }
  }

  private void executeRecursively(NodeWithId<N> node, Context context) {
    boolean hasBeenProcessed = this.processed.contains(node);
    if (!hasBeenProcessed) {
      Set<NodeWithId<N>> successors = this.graph.successors(node);
      for (NodeWithId<N> successor : successors) {
        if (!this.processed.contains(successor.node)) {
          executeRecursively(successor, context);
        }
      }
      consumeAndMarkAsProcessed(node, context);
    }
  }

  private void consumeAndMarkAsProcessed(NodeWithId<N> node, Context context) {
    if (this.processed.add(node.node)) {
      this.consumer.accept(node.node, context);
    }
  }

  public Set<N> getRoots() {
    return this.roots.stream().map(n -> n.node).collect(Collectors.toSet());
  }

  public Set<N> getLeaves() {
    return this.leaves.stream().map(n -> n.node).collect(Collectors.toSet());
  }
}
