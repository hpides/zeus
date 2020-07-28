package de.hpi.des.hdes.engine;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

import de.hpi.des.hdes.engine.graph.vulcano.VulcanoTopologyBuilder;
import de.hpi.des.hdes.engine.io.ListSink;
import de.hpi.des.hdes.engine.io.ListSource;
import de.hpi.des.hdes.engine.VulcanoEngine;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Slf4j
public class JobManagerTest {

    @Timeout(20)
    @Test
    void testJobManager() throws InterruptedException {
        final var list = new ArrayList<Integer>();
        for (int i = 0; i < 100_000; i++) {
            list.add(i);
        }

        // Query 1 Definition
        final var sourceIntQ1 = new ListSource<>(list);
        final LinkedList<Integer> resultsQ1 = new LinkedList<>();
        final var sinkQ1 = new ListSink<>(resultsQ1);
        final var builderQ1 = new VulcanoTopologyBuilder();
        builderQ1.streamOf(sourceIntQ1).map(i -> i + 1).to(sinkQ1);
        Query Q1 = new Query(builderQ1.build());

        // Query 2 Definition
        final LinkedList<Integer> resultsQ2 = new LinkedList<>();
        final var sinkQ2 = new ListSink<>(resultsQ2);
        final var builderQ2 = new VulcanoTopologyBuilder();
        builderQ2.streamOf(sourceIntQ1).map(i -> i * 2).to(sinkQ2);
        Query Q2 = new Query(builderQ2.build());

        // Query 3 Definition
        final LinkedList<Integer> resultsQ3 = new LinkedList<>();
        final var sinkQ3 = new ListSink<>(resultsQ3);
        final var builderQ3 = new VulcanoTopologyBuilder();
        builderQ3.streamOf(sourceIntQ1).map(i -> i * 10).to(sinkQ3);
        Query Q3 = new Query(builderQ3.build());

        // Run the Queries
        VulcanoEngine engine = new VulcanoEngine();
        JobManager jobManager = new JobManager(engine);
        jobManager.addQuery(Q1);
        engine.run();
        sleep(10);
        final var minDiff = resultsQ1.size();
        log.info("Min diff {}", minDiff);
        jobManager.addQuery(Q2, 50, ChronoUnit.MILLIS);
        jobManager.addQuery(Q3, 100, ChronoUnit.MILLIS);

        while ((!sourceIntQ1.isDone()) || !(resultsQ1.size() == list.size())) {
            log.info("Is Done: " + sourceIntQ1.isDone());
            log.info("Results Q1: " + resultsQ1.size());
            log.info("Results Q2: " + resultsQ2.size());
            log.info("Results Q3: " + resultsQ3.size());
            sleep(100);
        }

        assertThat(resultsQ1.size()).isEqualTo(list.size());
        assertThat(resultsQ1.size() - resultsQ2.size()).isGreaterThanOrEqualTo(minDiff);
        assertThat(resultsQ2.size() - resultsQ1.size()).isLessThanOrEqualTo(0);
        assertThat(resultsQ2.size() >= resultsQ3.size()).isTrue();
        log.info("Results Q1: " + resultsQ1.size());
        log.info("Results Q2: " + resultsQ2.size());
        log.info("Results Q3: " + resultsQ3.size());
    }
}
