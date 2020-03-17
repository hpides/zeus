package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.util.Objects;

public final class Auction {

  public long id;
  public long currentPrice;
  public long reserve;
  public String privacy;
  public long sellerId;
  public long category;
  public long quantity;
  public String type;
  public long startTime;
  public long endTime;
  public long eventTime;

  public Auction() {}

  public Auction(long id, long currentPrice, long reserve, String privacy, long sellerId, long category, long quantity,
                 String type, long startTime, long endTime, long eventTime) {
    this.id = id;
    this.currentPrice = currentPrice;
    this.reserve = reserve;
    this.privacy = privacy;
    this.sellerId = sellerId;
    this.category = category;
    this.quantity = quantity;
    this.type = type;
    this.startTime = startTime;
    this.endTime = endTime;
    this.eventTime = eventTime;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getCurrentPrice() {
    return currentPrice;
  }

  public void setCurrentPrice(long currentPrice) {
    this.currentPrice = currentPrice;
  }

  public long getReserve() {
    return reserve;
  }

  public void setReserve(long reserve) {
    this.reserve = reserve;
  }

  public String getPrivacy() {
    return privacy;
  }

  public void setPrivacy(String privacy) {
    this.privacy = privacy;
  }

  public long getSellerId() {
    return sellerId;
  }

  public void setSellerId(long sellerId) {
    this.sellerId = sellerId;
  }

  public long getCategory() {
    return category;
  }

  public void setCategory(long category) {
    this.category = category;
  }

  public long getQuantity() {
    return quantity;
  }

  public void setQuantity(long quantity) {
    this.quantity = quantity;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Auction auction = (Auction) o;
        return id == auction.id &&
                currentPrice == auction.currentPrice &&
                reserve == auction.reserve &&
                sellerId == auction.sellerId &&
                category == auction.category &&
                quantity == auction.quantity &&
                startTime == auction.startTime &&
                endTime == auction.endTime &&
                privacy.equals(auction.privacy) &&
                type.equals(auction.type);
    }

  @Override
  public String toString() {
    return "Auction{" +
        "id=" + id +
        ", currentPrice=" + currentPrice +
        ", reserve=" + reserve +
        ", privacy='" + privacy + '\'' +
        ", sellerId=" + sellerId +
        ", category=" + category +
        ", quantity=" + quantity +
        ", type='" + type + '\'' +
        ", startTime=" + startTime +
        ", endTime=" + endTime +
        ", eventTime=" + eventTime +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(id, currentPrice, reserve, privacy, sellerId, category, quantity, type, startTime,
            endTime, eventTime);
  }
}
