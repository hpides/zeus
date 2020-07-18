package de.hpi.des.hdes.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import de.hpi.des.hdes.engine.cstream.CStream;
import de.hpi.des.hdes.engine.generators.PrimitiveType;
import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;

public class FullPipelineTest {

    // @Test
    public void testAJoin() {
        List<Tuple2<Integer, Boolean>> source1 = new LinkedList<>();
        for (int i = 0; i <= 21; i++) {
            if (i % 5 == 1) {
                source1.add(new Tuple2<Integer, Boolean>(42, true));
            } else {
                source1.add(new Tuple2<Integer, Boolean>(i, false));
            }
        }
        List<Tuple2<Integer, Boolean>> source2 = new LinkedList<>();
        for (int i = 20; i <= 41; i++) {
            if (i % 5 == 1) {
                source2.add(new Tuple2<Integer, Boolean>(42, true));
            } else {
                source2.add(new Tuple2<Integer, Boolean>(i, false));
            }
        }
        VulcanoTopologyBuilder builder = new VulcanoTopologyBuilder();
        CStream stream1 = builder.streamOfC(source1, 4);
        List<Tuple4<Long, Integer, Integer, Boolean>> resultList = new LinkedList<>();
        builder.streamOfC(source2, 4).ajoin(stream1, new PrimitiveType[] { PrimitiveType.INT },
                new PrimitiveType[] { PrimitiveType.INT }, 0, 0, 5).toStaticList(resultList);

        JobManager manager = new JobManager(new CompiledEngine());
        manager.addQuery(builder.buildAsQuery());
        manager.runEngine();
        long startTime = System.currentTimeMillis();
        boolean timeout = false;
        while (resultList.size() < 4) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if (System.currentTimeMillis() - startTime > 500) {
                timeout = true;
                break;
            }
        }
        manager.shutdown();

        assertThat(resultList).containsExactlyInAnyOrderElementsOf(
                Arrays.asList(new Tuple4<Long, Integer, Integer, Boolean>(1L, 42, 42, false),
                        new Tuple4<Long, Integer, Integer, Boolean>(6L, 42, 42, false),
                        new Tuple4<Long, Integer, Integer, Boolean>(11L, 42, 42, false),
                        new Tuple4<Long, Integer, Integer, Boolean>(16L, 42, 42, false)));
        assertFalse(timeout, "Timeout during full pipeline ajoin");
    }
}