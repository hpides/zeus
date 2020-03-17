package de.hpi.des.hdes.benchmark.nexmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;
import de.hpi.des.hdes.benchmark.nexmark.protobuf.NextmarkScheme;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ObjectAuctionStreamGenerator {

  private static final String[] auction_type = {"Regular", "Featured", "Dutch"};
  public static int MAXAUCTIONLEN_SEC = 24 * 60 * 60; // 24 hours
  public static int MINAUCTIONLEN_SEC = 2 * 60 * 60; // 2 hours
  private Random rnd = new Random(1337);
  private long personIdCounter = 0;
  private long auctionIdCounter = 0;
  private long bidIdCounter = 0;
  private SimpleCalendar calendar = new SimpleCalendar(rnd);

  // otherwise these arrays might get much to large
  private List<Person> persons = new ArrayList<>();
  private List<Auction> auctions = new ArrayList<>();
  private DataGenerator dataGenerator = new DataGenerator();


  public Bid generateBid() {
    generatePersonIfNecessary();
    generateAuctionIfNecessary();
    Auction auction = auctions.get(rnd.nextInt(auctions.size()));
    long betterId = persons.get(rnd.nextInt(persons.size())).id;
    calendar.incrementTime();
    long time = calendar.getTimeInSecs();
    long bidPrice = auction.currentPrice + rnd.nextInt(25) + 1;
    long eventTime = System.nanoTime();

    Bid bid = new Bid(bidIdCounter,
            auction.id,
            betterId,
            time,
            bidPrice,
            eventTime);
    bidIdCounter++;
    return bid;
  }

  public Person generatePerson() {
    String phoneNumber = createPhoneNumber();
    String firstName = this.dataGenerator.getRandomFirstName();
    String lastName = this.dataGenerator.getRandomLastName();
    String domain = this.dataGenerator.getRandomEmail();
    String email = lastName.concat("@").concat(domain);
    String homepage = "https://www.".concat(domain).concat("/~").concat(lastName);
    String creditCard = createCreditCardNumber();

    String street = Integer.toString(rnd.nextInt(100)).concat(" ")
            .concat(this.dataGenerator.getRandomLastName()).concat(" Rd");
    String city = this.dataGenerator.getRandomCity();
    String country = this.dataGenerator.getRandomCountry();
    String province = this.dataGenerator.getRandomProvinces();
    String zipCode = String.valueOf(rnd.nextInt(99999) + 1);

    String education = this.dataGenerator.getRandomEducation();
    String gender = (rnd.nextInt(2) == 1) ? "male" : "female";
    String business = (rnd.nextInt(2) == 1) ? "Yes" : "No";
    String age = Integer.toString(rnd.nextInt(50) + 18);
    String income = String.valueOf((rnd.nextInt(65000) + 30000));

    long eventTime = System.nanoTime();

    Person person = new Person(personIdCounter,
            firstName.concat(" ").concat(lastName),
            email,
            phoneNumber,
            homepage,
            creditCard,
            street, city, country, province, zipCode,
            education, gender, business, age, income, eventTime);
    personIdCounter++;
    if (this.persons.size() > 10000) {
      this.persons = new ArrayList<>();
    }
    persons.add(person);
    return person;
  }

  public Auction generateAuction() {
    generatePersonIfNecessary();
    long currentPrice = rnd.nextInt(200) + 1;
    long reserve = (int) Math.round(currentPrice * (1.2 + (this.rnd.nextDouble() + 1)));
    String privacy = (rnd.nextInt(2) == 1) ? "Yes" : "No";
    long category = rnd.nextInt(303);
    long quantity = 1 + rnd.nextInt(10);
    String type = auction_type[rnd.nextInt(3)];
    long startTime = calendar.getTimeInSecs();
    long endTime = calendar.getTimeInSecs() + rnd.nextInt(MAXAUCTIONLEN_SEC) + MINAUCTIONLEN_SEC;
    long eventTime = System.nanoTime();

    Auction auction = new Auction(auctionIdCounter,
            currentPrice,
            reserve,
            privacy,
            persons.get(rnd.nextInt(persons.size())).id,
            category,
            quantity,
            type,
            startTime,
            endTime,
            eventTime);
    auctionIdCounter++;
    auctions.add(auction);
    return auction;
  }

  private String createPhoneNumber() {
    String phoneNumber = "+";
    phoneNumber += Integer.toString(rnd.nextInt(98) + 1);
    phoneNumber += "(" + (rnd.nextInt(989) + 10) + ")";
    phoneNumber += String.valueOf(rnd.nextInt(9864196) + 123457);
    return phoneNumber;
  }

  private String createCreditCardNumber() {
    String creditCard = "";
    creditCard += (rnd.nextInt(9000) + 1000) + " ";
    creditCard += (rnd.nextInt(9000) + 1000) + " ";
    creditCard += (rnd.nextInt(9000) + 1000) + " ";
    creditCard += String.valueOf(rnd.nextInt(9000) + 1000);
    return creditCard;
  }

  private void generatePersonIfNecessary() {
    if(this.persons.size() == 0) {
      this.generatePerson();
    }
  }

  private void generateAuctionIfNecessary() {
    if(this.auctions.size() == 0) {
      this.generateAuction();
    }
  }

  public NextmarkScheme.Bid generateProtobufBid() {
    Bid bid = generateBid();
    return NextmarkScheme.Bid.newBuilder()
            .setId(bid.id)
            .setAuctionId(bid.auctionId)
            .setBetterId(bid.betterId)
            .setTime(bid.time)
            .setBid((int) bid.bid)
            .setEventTime(System.nanoTime()).build();
  }

  public NextmarkScheme.Auction generateProtobufAuction() {
    Auction auction = generateAuction();
    return NextmarkScheme.Auction.newBuilder()
            .setId(auction.id)
            .setCurrentPrice((int) auction.currentPrice)
            .setReserve((int) auction.reserve)
            .setPrivacy(auction.privacy)
            .setSellerId(auction.sellerId)
            .setCategory((int) auction.category)
            .setQuantity((int) auction.quantity)
            .setType(auction.type)
            .setStartTime(auction.startTime)
            .setEndTime(auction.endTime)
            .setEventTime(System.nanoTime()).build();
  }

  public NextmarkScheme.Person generateProtobufPerson() {
    Person person = generatePerson();
    return NextmarkScheme.Person.newBuilder()
            .setId(person.id)
            .setName(person.name)
            .setEmail(person.email)
            .setPhone(person.phone)
            .setHomepage(person.homepage)
            .setCreditcard(person.creditcard)
            .setStreet(person.street)
            .setCity(person.city)
            .setProvince(person.province)
            .setCountry(person.country)
            .setZipCode(person.zipCode)
            .setEducation(person.education)
            .setGender(person.gender)
            .setBusiness(person.business)
            .setAge(person.age)
            .setIncome(person.income)
            .setEventTime(System.nanoTime()).build();
  }

  public static Bid protobufBidtoBid(NextmarkScheme.Bid bid) {
    return new Bid(bid.getId(), bid.getAuctionId(), bid.getBetterId(), bid.getTime(), bid.getBid(), bid.getEventTime());
  }

  public static Auction protobufAuctionToAuction(NextmarkScheme.Auction auction) {
    return new Auction(auction.getId(), auction.getCurrentPrice(), auction.getReserve(), auction.getPrivacy(),
            auction.getSellerId(), auction.getCategory(), auction.getQuantity(), auction.getType(),
            auction.getStartTime(), auction.getEndTime(), auction.getEventTime());
  }

  public static Person protobufPersonToPerson(NextmarkScheme.Person person) {
    return new Person(person.getId(), person.getName(), person.getEmail(), person.getPhone(), person.getHomepage(),
            person.getCreditcard(), person.getStreet(), person.getCity(), person.getProvince(), person.getCountry(),
            person.getZipCode(), person.getEducation(), person.getGender(), person.getBusiness(), person.getAge(),
            person.getIncome(), person.getEventTime());
  }

}
