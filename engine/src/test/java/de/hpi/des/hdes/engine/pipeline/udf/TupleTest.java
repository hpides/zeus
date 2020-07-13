package de.hpi.des.hdes.engine.pipeline.udf;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.pipeline.udf.Tuple;

public class TupleTest {
  @Test
  public void getIndexError() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.INT);
    assertThatThrownBy(() -> t1.get(2)).hasMessageContaining("out of bounds");
    assertThatThrownBy(() -> t1.get(-1)).hasMessageContaining("out of bounds");
  }

  @Test
  public void getSuccessful() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    Tuple t2 = t1.get(0);
    Tuple t3 = t1.get(1);
    assertThat(t1.getTypes()).hasSize(2);
    assertThat(t2.getTypes()).hasSize(1);
    assertThat(t3.getTypes()).hasSize(1);
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t2).isNotEqualTo(t3);
    assertThat(t1).isNotEqualTo(t3);
    assertThat(t2.getTypes()).containsExactly(PrimitiveType.INT);
    assertThat(t3.getTypes()).containsExactly(PrimitiveType.LONG);
  }

  @Test
  public void addAtIndexError() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    assertThatThrownBy(() -> t1.addAt(3, PrimitiveType.LONG, "")).hasMessageContaining("out of bounds");
    assertThatThrownBy(() -> t1.addAt(-1, PrimitiveType.LONG, "")).hasMessageContaining("out of bounds");
  }

  @Test
  public void addSuccessful() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    Tuple t2 = t1.addAt(1, PrimitiveType.LONG, "transformation");
    Tuple t3 = t1.add(PrimitiveType.INT, "transformation");
    assertThat(t1.getTypes()).hasSize(2);
    assertThat(t2.getTypes()).hasSize(3);
    assertThat(t3.getTypes()).hasSize(3);
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t2).isNotEqualTo(t3);
    assertThat(t1).isNotEqualTo(t3);
    assertThat(t2.getTypes()).containsExactly(PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.LONG);
    assertThat(t3.getTypes()).containsExactly(PrimitiveType.INT, PrimitiveType.LONG, PrimitiveType.INT);
  }

  @Test
  public void removeIndexError() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    assertThatThrownBy(() -> t1.remove(3)).hasMessageContaining("out of bounds");
    assertThatThrownBy(() -> t1.remove(-1)).hasMessageContaining("out of bounds");
  }

  @Test
  public void removeSuccesful() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    Tuple t2 = t1.remove(1);
    Tuple t3 = t1.remove(0);
    assertThat(t1.getTypes()).hasSize(2);
    assertThat(t2.getTypes()).hasSize(1);
    assertThat(t3.getTypes()).hasSize(1);
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t2).isNotEqualTo(t3);
    assertThat(t1).isNotEqualTo(t3);
    assertThat(t2.getTypes()).containsExactly(PrimitiveType.INT);
    assertThat(t3.getTypes()).containsExactly(PrimitiveType.LONG);
  }

  @Test
  public void mutateIndexError() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    assertThatThrownBy(() -> t1.mutateAt(3, PrimitiveType.LONG, "")).hasMessageContaining("out of bounds");
    assertThatThrownBy(() -> t1.mutateAt(-1, PrimitiveType.LONG, "")).hasMessageContaining("out of bounds");
  }

  @Test
  public void mutateSuccesful() {
    Tuple t1 = new Tuple(PrimitiveType.INT, PrimitiveType.LONG);
    Tuple t2 = t1.mutateAt(1, PrimitiveType.INT, "transformation");
    Tuple t3 = t1.mutateAt(0, PrimitiveType.INT, "transformation");
    assertThat(t1.getTypes()).hasSize(2);
    assertThat(t2.getTypes()).hasSize(2);
    assertThat(t3.getTypes()).hasSize(2);
    assertThat(t1).isNotEqualTo(t2);
    assertThat(t2).isNotEqualTo(t3);
    assertThat(t1).isNotEqualTo(t3);
    assertThat(t2.getTypes()).containsExactly(PrimitiveType.INT, PrimitiveType.INT);
    assertThat(t3.getTypes()).containsExactly(PrimitiveType.INT, PrimitiveType.LONG);
  }

  @Test
  public void testLastAndFirstAwareness() {
    Tuple t1 = new Tuple(PrimitiveType.LONG, PrimitiveType.INT);
    Tuple t2 = t1.get(1);
    Tuple t3 = t2.add(PrimitiveType.INT, "() -> 3");
    Tuple t4 = t3.mutateAt(1, PrimitiveType.INT, "() -> 1");
    assertThat(t1.isFirst()).isTrue();
    assertThat(t1.isLast()).isFalse();
    assertThat(t2.isFirst()).isFalse();
    assertThat(t2.isLast()).isFalse();
    assertThat(t3.isFirst()).isFalse();
    assertThat(t3.isLast()).isFalse();
    assertThat(t4.isFirst()).isFalse();
    assertThat(t4.isLast()).isTrue();
  }
}