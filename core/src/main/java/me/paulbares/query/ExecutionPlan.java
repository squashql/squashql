package me.paulbares.query;

import com.google.common.graph.Graph;
import me.paulbares.query.GraphDependencyBuilder.NodeWithId;

import java.util.HashSet;
import java.util.Set;

public class ExecutionPlan<N> {

  private final Graph<NodeWithId<N>> graph;
  private final Set<N> processed = new HashSet<>();
  private final Set<N> roots = new HashSet<>();
  private final Set<N> leaves = new HashSet<>();

  public ExecutionPlan(Graph<NodeWithId<N>> graph) {
    this.graph = graph;

    Set<NodeWithId<N>> nodes = this.graph.nodes();
    for (NodeWithId<N> node : nodes) {
      if (this.graph.outDegree(node) == 0) {
        this.leaves.add(node.node);
      }

      if (this.graph.inDegree(node) == 0) {
        this.roots.add(node.node);
      }
    }
  }

  public void execute() {
    this.processed.addAll(this.leaves);

    for (N node : this.roots) {
      executeRecursively(node);
    }
  }

  private void executeRecursively(N node) {
    boolean hasBeenProcessed = this.processed.contains(node);
    if (!hasBeenProcessed) {
      Set<NodeWithId<N>> successors = this.graph.successors(new NodeWithId(node, -1));
      for (NodeWithId<N> successor : successors) {
        if (!this.processed.contains(successor.node)) {
          executeRecursively(successor.node);
        }
      }
      executeNode(node);
      this.processed.add(node);
    }
  }

  protected void executeNode(N node) {
    // TODO
  }

  public Set<N> getRoots() {
    return this.roots;
  }

  public Set<N> getLeaves() {
    return this.leaves;
  }
}
