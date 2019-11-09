package de.hpi.des.mpws2019.benchmark;

import java.util.concurrent.ExecutionException;

public class Main {

  public static void main(final String[] args) throws InterruptedException, ExecutionException {
    final Benchmarker benchmarker = new Benchmarker();
    benchmarker.run();
  }

}
