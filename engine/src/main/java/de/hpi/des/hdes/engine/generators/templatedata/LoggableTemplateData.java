package de.hpi.des.hdes.engine.generators.templatedata;

import de.hpi.des.hdes.engine.execution.Dispatcher;
import lombok.Getter;

@Getter
public class LoggableTemplateData {
    private final boolean loggingEnabled = Dispatcher.LOGGING_ENABLED();
}