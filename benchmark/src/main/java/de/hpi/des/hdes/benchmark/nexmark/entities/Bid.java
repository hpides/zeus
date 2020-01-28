package de.hpi.des.hdes.benchmark.nexmark.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Bid {

  public long id;
  public long auctionId;
  public long betterId;
  public long time;
  public long bid;
}
