package de.hpi.des.mpws2019.engine.operation;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.mpws2019.engine.execution.slot.QueueConnector;
import de.hpi.des.mpws2019.engine.execution.slot.QueueBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class StreamFlatMapTest {

  @Test
  void shouldFlatMapElements() {
    List<Character> result = new LinkedList<>();

    final StreamFlatMap<String, Character> filter = new StreamFlatMap<>(string -> string.chars()
        .mapToObj(c -> (char) c)
        .collect(Collectors.toList()));
    
    filter.init(result::add);

    List.of("hel", "lo")
        .forEach(filter::process);

    assertThat(result).containsExactly('h', 'e', 'l', 'l', 'o');
  }
}