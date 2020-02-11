package de.hpi.des.hdes.engine.execution;

import java.util.concurrent.TimeUnit;
import lombok.Getter;

public class ExecutionConfig {
  @Getter
  private static RunConfig config = new DevConfig();

  private ExecutionConfig() {
    //noop
  }

  public static void makeShortTimoutConfig(){
    config = new ShortTimeoutConfig();
  }

  public static void makeDevConfig(){
    config = new DevConfig();
  }

  public interface RunConfig {
    int getChunkSize();
    long getFlushIntervallNS();
    default long getFlushIntervallMS(){
      return TimeUnit.NANOSECONDS.toMillis(getFlushIntervallNS());
    }
  }

  @Getter
  public static class DevConfig implements RunConfig {
    private final int chunkSize = 10_000;
    private final long flushIntervallNS = 10_000_000;
  }

  @Getter
  public static class ShortTimeoutConfig implements RunConfig{
    private final  int chunkSize = 1;
    private final long flushIntervallNS = 1;
  }
}
