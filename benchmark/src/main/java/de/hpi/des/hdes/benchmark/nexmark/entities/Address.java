package de.hpi.des.hdes.benchmark.nexmark.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Address {

  public String street;
  public String city;
  public String province;
  public String country;
  public String zipCode;
}
