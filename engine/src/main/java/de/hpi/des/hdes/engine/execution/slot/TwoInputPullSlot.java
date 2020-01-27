package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.graph.Node;
import de.hpi.des.hdes.engine.operation.OneInputOperator;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import de.hpi.des.hdes.engine.operation.Collector;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TwoInputPullSlot<IN1, IN2, OUT> extends RunnableSlot<OUT> {
  private final Slot<IN1> parent1;
  private final Slot<IN2> parent2;
  private final Buffer<IN1> input1;
  private final Buffer<IN2> input2;
  private final TwoInputOperator<IN1, IN2, OUT> operator;
  private final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode;

  public TwoInputPullSlot(final TwoInputOperator<IN1, IN2, OUT> operator,
                          final Slot<IN1> parent1, final Slot<IN2> parent2,
                          final Buffer<IN1> input1, final Buffer<IN2> input2,
                          final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
    this.operator = operator;
    this.parent1 = parent1;
    this.parent2 = parent2;
    this.input1 = input1;
    this.input2 = input2;
    this.binaryOperationNode = binaryOperationNode;
  }

  public void runStep() {
    final IN1 in1 = this.input1.poll();
    if (in1 != null) {
      this.operator.processStream1(in1);
    }
    final IN2 in2 = this.input2.poll();
    if (in2 != null) {
      this.operator.processStream2(in2);
    }
  }

  @Override
  public BinaryOperationNode<IN1, IN2, OUT> getTopologyNode() {
    return this.binaryOperationNode;
  }

}