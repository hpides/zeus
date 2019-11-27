package de.hpi.des.mpws2019.benchmark.generator;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.jooq.lambda.Seq.seq;

import de.hpi.des.mpws2019.benchmark.TimedBlockingSource;
import de.hpi.des.mpws2019.benchmark.TupleEvent;
import de.hpi.des.mpws2019.engine.graph.TopologyBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.commons.math3.stat.inference.ChiSquareTest;
import org.junit.jupiter.api.Test;

class UniformGeneratorTest {

  @Test
  void testUniformGeneration() throws ExecutionException, InterruptedException {
    final int eventsPerSecond = 1000;
    final int timeInSeconds = 3;
    final int totalEvents = 3000;

    UniformGenerator generator = new UniformGenerator(
        eventsPerSecond,
        timeInSeconds,
        Executors.newSingleThreadExecutor()
    );

    final TimedBlockingSource<TupleEvent> timedQueue = new TimedBlockingSource<>();


    CompletableFuture<Boolean> successFuture = generator.generate(timedQueue);
    // to block until generation finished
    successFuture.get();
    assertThat(timedQueue.getQueue().size() == totalEvents);

    final List<Long> times = seq(timedQueue.getBenchmarkCheckpointToAddTime())
        .map(e -> e.v2)
        .toList();

    final Long minTime = Collections.min(times);
    final Long maxTime = Collections.max(times);
    final int bins = 30;
    final double binSize = (maxTime - minTime)/Double.valueOf(bins);
    final double[] expectedPerBin = new double[bins];
    long[] countPerBin = new long[bins];
    Arrays.fill(expectedPerBin, totalEvents/bins);

    times.forEach(time -> {
      /*
       maxTime would fall into the last bin + 1. Therefore we have to check for it and put it
       in the last bin.
       */
      if (time == maxTime) {
        countPerBin[bins-1]++;
      } else {
        final int targetBin = (int) ((time - minTime)/binSize);
        countPerBin[targetBin]++;
      }
    });

    /*
     Null hypothesis: Data is drawn from a uniform distribution.
     The chi square test will return true i.e. reject the null hypothesis if the values
     are not drawn from a uniform distribution. In this case we can be pretty sure that our
     data generator is generating data uniformly over the time interval.
     */
    var chiSquare = new ChiSquareTest();
    boolean isRejected = chiSquare.chiSquareTest(expectedPerBin, countPerBin, 0.05);
    assertThat(!isRejected);
  }
}