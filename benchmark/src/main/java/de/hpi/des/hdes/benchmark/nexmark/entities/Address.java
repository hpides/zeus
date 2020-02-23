package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Address implements Serializable {

  public String street;
  public String city;
  public String province;
  public String country;
  public String zipCode;
}
