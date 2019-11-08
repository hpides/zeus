package de.hpi.des.mpws2019.benchmark;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataGenerator {

    private int sleepTimePerLoop;
    private int eventsPerSecond;
    private long idCounter = 0;
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public DataGenerator(int sleepTimePerLoop, int eventsPerSecond) {
        sleepTimePerLoop = sleepTimePerLoop;
        eventsPerSecond = eventsPerSecond;
    }

    private TupleEvent generateRandomIntTuple() {
        Random r = new Random();
        TupleEvent event = new TupleEvent(idCounter, r.nextInt(10000));
        idCounter++;
        return event;
    }

    private void sleepFor(int ms) {
        try {
            Thread.sleep(sleepTimePerLoop);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Future<Integer> generateDataSimple(int numberOfEvents, Queue<TupleEvent> queue) {
        return executor.submit(() -> sendEventsSimple(numberOfEvents, queue));
    }

    public Future<Integer> generateDataTimeAware(int numberOfEvents, Queue<TupleEvent> queue) {
        return executor.submit(() -> sendEventsTimeAware(numberOfEvents, queue));
    }

    private Integer sendEventsSimple(int numberOfEvents, Queue<TupleEvent> queue) {
        int sendEvents = 0;
        while(sendEvents < numberOfEvents) {
            sleepFor(sleepTimePerLoop);
            queue.add(generateRandomIntTuple());
            sendEvents++;
        }
        log.info("Finished sending events");
        return sendEvents;
    }

    private Integer sendEventsTimeAware(int numberOfEvents, Queue<TupleEvent> queue) {
        int sendEvents = 0;
        long startTime = System.nanoTime();
        while(sendEvents < numberOfEvents) {
            sleepFor(this.sleepTimePerLoop);
            long timeNow = System.nanoTime();
            long difference = (timeNow - startTime) / (1^9);
            int eventsToBeSent = (int) difference * eventsPerSecond;

            if(eventsToBeSent > 0) {
                // Ensure that we do not exceed the maxium amout of events
                if (eventsToBeSent + sendEvents > numberOfEvents) {
                    eventsToBeSent = numberOfEvents - sendEvents;
                }

                // Send the events
                for (int i = 0; i < eventsToBeSent; i++) {
                    queue.add(generateRandomIntTuple());
                }

                sendEvents += eventsToBeSent;
                startTime = timeNow;
            }
        }
        log.info("Finished sending events");
        return sendEvents;
    }

}
