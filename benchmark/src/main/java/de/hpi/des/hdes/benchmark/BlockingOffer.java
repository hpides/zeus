package de.hpi.des.hdes.benchmark;

public interface BlockingOffer<E> {

   void offer(E event) throws IllegalStateException;

   void flush();
}
