package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.util.Objects;

public final class Bid {
  public long id;
  public long auctionId;
  public long betterId;
  public long time;
  public long bid;
  public long eventTime;

  public Bid() {}

  public Bid(long id, long auctionId, long betterId, long time, long bid, long eventTime) {
    this.id = id;
    this.auctionId = auctionId;
    this.betterId = betterId;
    this.time = time;
    this.bid = bid;
    this.eventTime = eventTime;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getAuctionId() {
    return auctionId;
  }

  public void setAuctionId(long auctionId) {
    this.auctionId = auctionId;
  }

  public long getBetterId() {
    return betterId;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public void setBetterId(long betterId) {
    this.betterId = betterId;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getBid() {
    return bid;
  }

  public void setBid(long bid) {
    this.bid = bid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Bid bid1 = (Bid) o;
    return id == bid1.id &&
            auctionId == bid1.auctionId &&
            betterId == bid1.betterId &&
            time == bid1.time &&
            bid == bid1.bid;
  }

  @Override
  public String toString() {
    return "Bid{" +
        "id=" + id +
        ", auctionId=" + auctionId +
        ", betterId=" + betterId +
        ", time=" + time +
        ", bid=" + bid +
        ", eventTime=" + eventTime +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, auctionId, betterId, time, bid, eventTime);
  }

}


