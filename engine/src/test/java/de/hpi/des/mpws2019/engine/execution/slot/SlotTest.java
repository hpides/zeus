package de.hpi.des.mpws2019.engine.execution.slot;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.operation.StreamJoin;
import de.hpi.des.mpws2019.engine.operation.StreamMap;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SlotTest {

  @Test
  void shouldExecuteOperatorsCorrectly() {
    final var queueInput = new QueueInput<>(new LinkedList<>(List.of(0, 1, 3, 4)));
    final var map1 = new StreamMap<Integer, Integer>(a -> a + 1);
    final var out1 = new QueueBuffer<Integer>();

    final var slot1 = new OneInputSlot<>(map1, queueInput, out1);

    final var join = new StreamJoin<Integer, Integer, Integer>((i, i2) -> i, Integer::equals);

    final var out2 = new QueueBuffer<Integer>();

    final var slot2 = new TwoInputSlot<>(
        join,
        new QueueInput<>(out1.getQueue()),
        new QueueInput<>(new LinkedList<>(List.of(1, 2, 5, 6, 7))),
        out2);

    runAllSlots(slot1, slot2);
    assertThat(out2.getQueue()).containsExactlyElementsOf(List.of(1));
    runAllSlots(slot1, slot2);
    assertThat(out2.getQueue()).containsExactlyElementsOf(List.of(1, 2));
    runAllSlots(slot1, slot2);
    assertThat(out2.getQueue()).containsExactlyElementsOf(List.of(1, 2));
    runAllSlots(slot1, slot2);
    assertThat(out2.getQueue()).containsExactlyElementsOf(List.of(1, 2, 5));
    runAllSlots(slot1, slot2);
    assertThat(out2.getQueue()).containsExactlyElementsOf(List.of(1, 2, 5));

  }

  private static void runAllSlots(final OneInputSlot<Integer, Integer> slot1,
                                  final TwoInputSlot<Integer, Integer, Integer> slot2) {
    slot1.run();
    slot2.run();
  }
}