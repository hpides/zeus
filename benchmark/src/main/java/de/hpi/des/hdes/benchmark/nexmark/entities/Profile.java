package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public final class Profile implements Serializable {

  public String education;
  public String gender;
  public String business;
  public String age;
  public String income;
}
