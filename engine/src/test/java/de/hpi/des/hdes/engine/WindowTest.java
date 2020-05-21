package de.hpi.des.hdes.engine;

import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.aggregators.SumAggregator;
import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import de.hpi.des.hdes.engine.execution.plan.VulcanoExecutionPlan;
import de.hpi.des.hdes.engine.execution.slot.VulcanoRunnableSlot;
import de.hpi.des.hdes.engine.graph.TopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.operation.Sink;
import de.hpi.des.hdes.engine.operation.Source;
import de.hpi.des.hdes.engine.stream.AStream;
import de.hpi.des.hdes.engine.udf.Aggregator;
import de.hpi.des.hdes.engine.window.Time;
import de.hpi.des.hdes.engine.window.WatermarkGenerator;
import de.hpi.des.hdes.engine.window.assigner.TumblingEventTimeWindow;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
class WindowTest extends ShortTimoutSetup {

    @BeforeAll
    static void beforeAll() {
        ExecutionConfig.makeShortTimoutConfig();
    }

    @Test
    void testTumblingEventTimeWindow() {
        final List<Integer> list = List.of(1, 2, 3, 100);
        final Source<Integer> source = new ListSource<>(list, new WatermarkGenerator<>(0, 1), i -> i + 1);
        final Source<String> stringSource = new ListSource<>(List.of("2", "3", "4", "100"),
                new WatermarkGenerator<>(0, 1), Integer::valueOf);

        final List<String> result = new LinkedList<>();
        final Sink<String> sink = new ListSink<>(result);

        final TopologyBuilder builder = new TopologyBuilder();
        final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
        final AStream<String> stringString = builder.streamOf(stringSource);

        stream.window(new TumblingEventTimeWindow(1))
                .join(stringString, (i, s) -> s + i, String::valueOf, i -> i, new WatermarkGenerator<>(0, 1), e -> 0)
                .to(sink);

        final Query query = new Query(builder.build());
        final List<VulcanoRunnableSlot<?>> slots = VulcanoExecutionPlan.createPlan(query).getRunnableSlots();

        TestUtil.runAndTick(slots);
        assertThat(result).containsExactly("22");
        TestUtil.runAndTick(slots);
        assertThat(result).containsExactly("22", "33");
        TestUtil.runAndTick(slots);
        assertThat(result).containsExactly("22", "33", "44");
        TestUtil.runAndTick(slots);
        assertThat(result).containsExactly("22", "33", "44");
    }

    @Test
    void testTumblingEventTimeWindowWithLateness() {
        final List<Integer> list = List.of(1, 1, 2, 1, 1, 100);
        final Source<Integer> source = new ListSource<>(list, new WatermarkGenerator<>(1, 1), i -> i + 1);
        final Source<String> stringSource = new ListSource<>(List.of("2", "3", "4", "100"),
                new WatermarkGenerator<>(0, 1), Integer::valueOf);

        final List<String> result = new LinkedList<>();
        final Sink<String> sink = new ListSink<>(result);

        final TopologyBuilder builder = new TopologyBuilder();
        final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);
        final AStream<String> stringString = builder.streamOf(stringSource);

        stream.window(new TumblingEventTimeWindow(1))
                .join(stringString, (i, s) -> s + i, String::valueOf, i -> i, new WatermarkGenerator<>(2, 1), e -> 0)
                .to(sink);

        final Query query = new Query(builder.build());

        final List<VulcanoRunnableSlot<?>> slots = VulcanoExecutionPlan.createPlan(query).getRunnableSlots();

        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        assertThat(result).containsExactly("22", "22", "33");

    }

    @Test
    void testKeyedAggregation() throws InterruptedException {
        Tuple2<Integer, Integer> t12 = new Tuple2<>(1, 2);
        Tuple2<Integer, Integer> t23 = new Tuple2<>(2, 3);
        Tuple2<Integer, Integer> t34 = new Tuple2<>(3, 4);
        Tuple2<Integer, Integer> t45 = new Tuple2<>(4, 5);
        Tuple2<Integer, Integer> closeLastWindowEvent = new Tuple2<>(-1, -1);

        final List<Tuple2<Integer, Integer>> list = List.of(t12, t12, t23, t23, t34, t34, t45, t45,
                closeLastWindowEvent);
        final Source<Tuple2<Integer, Integer>> source = new ListSource<>(list, new WatermarkGenerator<>(0, 1),
                e -> System.nanoTime());

        final List<Integer> result = new LinkedList<>();
        final Sink<Integer> sink = new ListSink<>(result);

        final TopologyBuilder builder = new TopologyBuilder();
        builder.streamOf(source).window(new TumblingEventTimeWindow(Time.of(200).getNanos())).groupBy(t -> t.v1)
                .aggregate(new Aggregator<Tuple2<Integer, Integer>, Integer, Integer>() {
                    @Override
                    public Integer initialize() {
                        return 0;
                    }

                    @Override
                    public Integer add(Integer state, Tuple2<Integer, Integer> input) {
                        return state + input.v2;
                    }

                    @Override
                    public Integer getResult(Integer state) {
                        return state;
                    }
                }, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime()).to(sink);

        final Query query = new Query(builder.build());

        var slots = VulcanoExecutionPlan.createPlan(query).getRunnableSlots();

        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        Thread.sleep(250);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        TestUtil.runAndTick(slots);
        assertThat(result).containsExactlyInAnyOrder(4, 6, 8);
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime() / 1000);
        Thread.sleep(250);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        result.removeIf(t -> t == -1);
        assertThat(result).containsExactlyInAnyOrder(4, 6, 8, 10);
    }

    @Test
    void testAggregation() throws InterruptedException {
        final List<Integer> list = List.of(1, 2, 3, 4, 5, 6, -1);
        final Source<Integer> source = new ListSource<>(list, new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime());

        final List<Integer> result = new LinkedList<>();
        final Sink<Integer> sink = new ListSink<>(result);

        final TopologyBuilder builder = new TopologyBuilder();
        final AStream<Integer> stream = builder.streamOf(source).map(i -> i + 1);

        stream.window(new TumblingEventTimeWindow(Time.of(200).getNanos()))
                .aggregate(new SumAggregator(), new WatermarkGenerator<>(0, 1), (e) -> System.nanoTime()).to(sink);

        final Query query = new Query(builder.build());

        var slots = VulcanoExecutionPlan.createPlan(query).getRunnableSlots();

        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> i.toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime());
        Thread.sleep(203);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        TestUtil.runAndTick(slots);
        Thread.sleep(203);
        TestUtil.runAndTick(slots);
        log.debug("Result: " + result.stream().map(i -> toString()).collect(Collectors.joining(", ")));
        log.debug("Nanotime: " + System.nanoTime() / 1000);
        assertThat(result).containsExactly(9, 18);
    }

}
