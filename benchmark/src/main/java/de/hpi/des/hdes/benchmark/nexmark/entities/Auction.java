package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Auction implements Serializable {

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
}
