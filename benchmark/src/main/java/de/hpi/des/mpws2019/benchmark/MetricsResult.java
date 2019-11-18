package de.hpi.des.mpws2019.benchmark;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class MetricsResult {
    private long totalEvents;

    private double averageEventTimeLatency;
    private double maxEventTimeLatency;
    private double minEventTimeLatency;

    private double averageProcessingTimeLatency;
    private double maxProcessingTimeLatency;
    private double minProcessingTimeLatency;
}
