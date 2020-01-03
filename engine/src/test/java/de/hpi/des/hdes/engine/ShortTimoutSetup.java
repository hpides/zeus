package de.hpi.des.hdes.engine;

import de.hpi.des.hdes.engine.execution.ExecutionConfig;
import org.junit.jupiter.api.BeforeAll;

public class ShortTimoutSetup {

  @BeforeAll
  static void beforeAll() {
    ExecutionConfig.makeShortTimoutConfig();
  }
}
