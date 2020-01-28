package de.hpi.des.hdes.benchmark.nexmark.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Profile {

  public String education;
  public String gender;
  public String business;
  public String age;
  public String income;
}
