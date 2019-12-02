package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.benchmark.generator.Generator;
import lombok.Value;

@Value
public final class BenchmarkResult {

    private final Generator usedGenerator;
    private final TimedBlockingSource timedSource;
    private final TimedBlockingSink timedSink;

}
