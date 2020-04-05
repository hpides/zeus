package de.hpi.des.hdes.benchmark.nexmark;

import java.util.Random;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well44497b;
import org.jooq.lambda.tuple.Tuple5;

public class NexmarkLightDataGenerator {
  private long lastBidId = -1;
  private long lastAuctionId = -1;

  private Random random;
  private NormalDistribution ndPrice;
  private NormalDistribution ndBetterId;
  private NormalDistribution ndAuctionId;

  public NexmarkLightDataGenerator(int seed) {
    this.random = new Random(seed);
    RandomGenerator randomGenerator = new Well44497b(seed);
    this.ndPrice = new NormalDistribution(randomGenerator, 5000, 1500);
    this.ndBetterId = new NormalDistribution(randomGenerator,50000, 15000);
    this.ndAuctionId = new NormalDistribution(randomGenerator,50000, 15000);

  }

  public Tuple5<Long, Long, Integer, Integer, Long> generateBid() {
    long bidId = getBidId();
    long auctionId = getAuctionIdtoBeton();
    int betterId = getBetterId();
    int price = getPrice();
    long eventTime = System.currentTimeMillis();
    return new Tuple5<>(bidId, auctionId, betterId, price, eventTime);
  }

  public Tuple5<Long, Integer, Integer, Integer, Long> generateAuction() {
    long auctionId = getAuctionId();
    int quantity = random.nextInt(99)+1;
    int type = random.nextInt(20);
    int minimumPrice = getPrice();
    long eventTime = System.currentTimeMillis();
    return new Tuple5<>(auctionId, quantity, type, minimumPrice, eventTime);
  }

  private long getBidId() {
    lastBidId += 1;
    return lastBidId;
  }

  private long getAuctionId() {
    lastAuctionId += 1;
    return lastAuctionId;
  }

  private int getBetterId() {
    int betterId = 0;
    while(betterId <= 0) {
      betterId = (int) ndBetterId.sample();
    }
    return betterId;
  }

  private int getPrice() {
    int price = 0;
    while(price <= 0) {
      price = (int) ndPrice.sample();
    }
    return price;
  }

  private long getAuctionIdtoBeton() {
    int normalValue = (int) ndAuctionId.sample();
    return Math.max(0, lastAuctionId - normalValue);
  }
}
