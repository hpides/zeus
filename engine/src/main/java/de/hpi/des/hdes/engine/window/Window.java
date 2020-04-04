package de.hpi.des.hdes.engine.window;

/**
 * Interface that windows have to implement.
 */
public interface Window {

  /**
   * Retrieves max. timestamp in the window.
   *
   * @return latest timestamp that is still part of the window.
   */
  long getMaxTimestamp();
}
