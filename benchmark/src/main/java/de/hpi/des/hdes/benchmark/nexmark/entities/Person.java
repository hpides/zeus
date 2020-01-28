package de.hpi.des.hdes.benchmark.nexmark.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Person {

  public long id;
  public String name;
  public String email;
  public String phone;
  public String homepage;
  public String creditcard;
  public Profile profile;
  public Address address;
}
