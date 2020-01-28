package de.hpi.des.hdes.engine.operation;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.AData;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class StreamFlatMapTest {

  @Test
  void shouldFlatMapElements() {
    List<Character> result = new LinkedList<>();

    final StreamFlatMap<String, Character> filter = new StreamFlatMap<>(string -> string.chars()
        .mapToObj(c -> (char) c)
        .collect(Collectors.toList()));

    filter.init(e -> result.add(e.getValue()));

    List.of("hel", "lo")
        .forEach(e -> filter.process(AData.of(e)));

    assertThat(result).containsExactly('h', 'e', 'l', 'l', 'o');
  }
}