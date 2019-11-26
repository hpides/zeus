package de.hpi.des.mpws2019.engine.execution.slot;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.operation.StreamJoin;
import de.hpi.des.mpws2019.engine.operation.StreamMap;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SlotTest {

  @Test
  void shouldExecuteOperatorsCorrectly() {
    final QueueBuffer<Integer> input = new QueueBuffer<>(new LinkedList<>(List.of(0, 1, 3, 4)));

    final var out1 = new QueueConnector<Integer>();
    final QueueBuffer out1buffer = out1.addQueueBuffer(UUID.fromString("00000000-0000-0000-0000-000000000000"));
    final StreamMap<Integer, Integer> map1 = new StreamMap<>(a -> a + 1);
    final var slot1 = new OneInputSlot<>(map1, input, out1);


    final QueueConnector<Integer> out2 = new QueueConnector<>();
    final QueueBuffer out2buffer = out2.addQueueBuffer(UUID.fromString("00000000-0000-0000-0000-000000000000"));

    final var join = new StreamJoin<Integer, Integer, Integer>((i, i2) -> i, Integer::equals);
    final var slot2 = new TwoInputSlot<>(
        join,
        out1buffer,
        new QueueBuffer<>(new LinkedList<>(List.of(1, 2, 5, 6, 7))), out2);

    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1));
    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1, 2));
    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1, 2));
    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1, 2, 5));
    runAllSlots(slot1, slot2);
    assertThat(out2buffer.getQueue()).containsExactlyElementsOf(List.of(1, 2, 5));
  }

  private void runAllSlots(final OneInputSlot<Integer, Integer> slot1,
                                  final TwoInputSlot<Integer, Integer, Integer> slot2) {
    slot1.run();
    slot2.run();
  }
}