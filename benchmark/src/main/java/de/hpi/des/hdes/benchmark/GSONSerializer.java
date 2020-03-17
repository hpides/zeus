package de.hpi.des.hdes.benchmark;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import java.lang.reflect.Type;
import org.jooq.lambda.tuple.Tuple2;

public class GSONSerializer<T> extends AbstractSerializer<T> {

  Gson gson;
  Type type;

  public GSONSerializer(Type type) {
    this.type = type;
    this.gson = new Gson();
  }

  public static GSONSerializer<Tuple2<Integer, Long>> forIntTuple() {
    return new GSONSerializer<>(new TypeToken<Tuple2<Integer, Long>>() {
    }.getType());
  }

  public static GSONSerializer<Bid> forBid() {
    return new GSONSerializer<>(new TypeToken<Bid>() {
    }.getType());
  }

  public static GSONSerializer<Auction> forAuction() {
    return new GSONSerializer<>(new TypeToken<Auction>() {
        }.getType());
    }

    public static GSONSerializer<Person> forPerson() {
        return new GSONSerializer<>(new TypeToken<Person>() {
        }.getType());
    }

    @Override
    public String serialize(T obj) {
        return gson.toJson(obj);
    }

    @Override
    public T deserialize(String obj) {
        return gson.fromJson(obj, this.type);
    }
}
