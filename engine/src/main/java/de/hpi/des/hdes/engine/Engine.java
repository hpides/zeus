package de.hpi.des.hdes.engine;

public interface Engine {

    public void addQuery(final Query query);

    public void deleteQuery(final Query query);

    public void shutdown();
}