package de.hpi.des.hdes.engine.shared.aggregation;

import java.util.BitSet;
import java.util.List;
import lombok.Value;

@Value
public class QueryChangelog {

  List<SharedQuery> queries;
  BitSet changelogCode;
}
