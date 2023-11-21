package io.squashql.query;


import io.squashql.query.DependencyGraph.NodeWithId;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ExecutionPlan<N> {

  private final Consumer<N> consumer;
  private final DependencyGraph<N> graph;
  private final Set<N> processed = new HashSet<>();
  private final Set<NodeWithId<N>> roots = new HashSet<>();
  private final Set<NodeWithId<N>> leaves = new HashSet<>();

  public ExecutionPlan(DependencyGraph<N> graph, Consumer<N> consumer) {
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

  public void execute() {
    for (NodeWithId<N> node : this.roots) {
      executeRecursively(node);
    }
  }

  private void executeRecursively(NodeWithId<N> node) {
    boolean hasBeenProcessed = this.processed.contains(node);
    if (!hasBeenProcessed) {
      Set<NodeWithId<N>> successors = this.graph.successors(node);
      for (NodeWithId<N> successor : successors) {
        if (!this.processed.contains(successor.node)) {
          executeRecursively(successor);
        }
      }
      consumeAndMarkAsProcessed(node);
    }
  }

  private void consumeAndMarkAsProcessed(NodeWithId<N> node) {
    if (this.processed.add(node.node)) {
      this.consumer.accept(node.node);
    }
  }

  public Set<N> getRoots() {
    return this.roots.stream().map(n -> n.node).collect(Collectors.toSet());
  }

  public Set<N> getLeaves() {
    return this.leaves.stream().map(n -> n.node).collect(Collectors.toSet());
  }
}
