package de.hpi.des.hdes.engine.execution.slot;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.execution.connector.Buffer;
import de.hpi.des.hdes.engine.execution.connector.Connector;
import de.hpi.des.hdes.engine.execution.connector.QueueBuffer;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.operation.StreamJoin;
import de.hpi.des.hdes.engine.operation.StreamMap;
import de.hpi.des.hdes.engine.window.GlobalTimeWindow;
import de.hpi.des.hdes.engine.window.assigner.GlobalWindow;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SlotTest {

  @Test
  void shouldExecuteOperatorsCorrectly() {
    final Source<Integer> source = new ListSource(new LinkedList<>(List.of(0, 1, 3, 4)));

    final StreamMap<Integer, Integer> map1 = new StreamMap<>(a -> a + 1);
    final Connector<Integer> outSource1 = new Connector();
    outSource1.addFunction(new DummyNode(), map1::process);
    source.init(outSource1);
    final Connector outMap1 = new Connector<Integer>();
    final Buffer outMap1Buffer = outMap1.addBuffer(new DummyNode());
    map1.init(outMap1);
    final Slot slot1 = new SourceSlot<>(source, outSource1);

    final Connector<Integer> out2 = new Connector<>();
    final QueueBuffer out2buffer = (QueueBuffer) out2.addBuffer(new DummyNode());

    final var join = new StreamJoin<Integer, Integer, Integer, GlobalTimeWindow>((i, i2) -> i, Integer::equals,
        GlobalWindow.create());
    join.init(out2);
    final var slot2 = new TwoInputSlot<>(
            join,
            outMap1Buffer,
            new QueueBuffer(new LinkedList<>(List.of(1, 2, 5, 6, 7))),
            out2);

    runAllSlots(slot1, slot2);
    runAllSlots(slot1, slot2);
    runAllSlots(slot1, slot2);
    runAllSlots(slot1, slot2);
    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1, 2, 5));
  }

  private void runAllSlots(final Slot slot1,
                                  final Slot slot2) {
    slot1.runStep();
    slot2.runStep();
  }
}