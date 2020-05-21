package de.hpi.des.hdes.engine;

public interface Engine {
    public void run();

    public void addQuery(final Query query);

    public void deleteQuery(final Query query);

    public void shutdown();

    public boolean isRunning();
}