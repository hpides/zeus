package de.hpi.des.hdes.benchmark.generator;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.jooq.lambda.tuple.Tuple1;

public class StringTupleGenerator extends UniformGenerator<Tuple1<String>> {

  private final Random random = new Random();

  public StringTupleGenerator(int eventsPerSecond, int timeInSeconds, ExecutorService executor) {
    super(eventsPerSecond, timeInSeconds, executor);
  }

  @Override
  protected Tuple1<String> generateEvent(boolean isBenchmark) {
    return new Tuple1<>(createCreditCardNumber());
  }

  private String createCreditCardNumber() {
    String creditCard = "";
    creditCard += (random.nextInt(9000) + 1000) + "-";
    creditCard += (random.nextInt(9000) + 1000) + "-";
    creditCard += (random.nextInt(9000) + 1000) + "-";
    creditCard += String.valueOf(random.nextInt(9000) + 1000);
    return creditCard;
  }

}
