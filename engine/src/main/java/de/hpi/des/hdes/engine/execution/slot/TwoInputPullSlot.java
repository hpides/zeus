package de.hpi.des.hdes.engine.execution.slot;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.graph.BinaryOperationNode;
import de.hpi.des.hdes.engine.operation.TwoInputOperator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TwoInputPullSlot<IN1, IN2, OUT> extends RunnableSlot<OUT> {

  private final Slot<IN1> parent1;
  private final Slot<IN2> parent2;
  private final Buffer<AData<IN1>> input1;
  private final Buffer<AData<IN2>> input2;
  private final TwoInputOperator<IN1, IN2, OUT> operator;
  private final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode;

  public TwoInputPullSlot(final TwoInputOperator<IN1, IN2, OUT> operator,
      final Slot<IN1> parent1, final Slot<IN2> parent2,
      final Buffer<AData<IN1>> input1, final Buffer<AData<IN2>> input2,
      final BinaryOperationNode<IN1, IN2, OUT> binaryOperationNode) {
    this.operator = operator;
    this.parent1 = parent1;
    this.parent2 = parent2;
    this.input1 = input1;
    this.input2 = input2;
    this.binaryOperationNode = binaryOperationNode;
  }

  public void runStep() {
    final AData<IN1> in1 = this.input1.poll();
    if (in1 != null) {
      this.operator.processStream1(in1);
    }
    final AData<IN2> in2 = this.input2.poll();
    if (in2 != null) {
      this.operator.processStream2(in2);
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
    if (this.input1.unsafePollAll().size() > 0 || this.input2.unsafePollAll().size() > 0) {
      log.warn("TwoInputSlot shut down but input buffers were not empty! {} and {} elements left",
          this.input1.unsafePollAll().size(), this.input2.unsafePollAll().size());
    }
  }

  @Override
  protected void cleanUp() {
    this.operator.close();
    super.cleanUp();
  }

  @Override
  public BinaryOperationNode<IN1, IN2, OUT> getTopologyNode() {
    return this.binaryOperationNode;
  }

}