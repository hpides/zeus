package de.hpi.des.hdes.benchmark.nexmark;

import de.hpi.des.hdes.benchmark.nexmark.entities.Auction;
import de.hpi.des.hdes.benchmark.nexmark.entities.Bid;
import de.hpi.des.hdes.benchmark.nexmark.entities.Person;

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
    Bid bid = new Bid(bidIdCounter,
        auction.id,
        betterId,
        time,
        bidPrice);
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

    Person person = new Person(personIdCounter,
        firstName.concat(" ").concat(lastName),
        email,
        phoneNumber,
        homepage,
        creditCard,
        street,city,country,province,zipCode,
        education,gender,business,age,income);
    personIdCounter++;
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

    Auction auction = new Auction(auctionIdCounter,
        currentPrice,
        reserve,
        privacy,
        persons.get(rnd.nextInt(persons.size())).id,
        category,
        quantity,
        type,
        startTime,
        endTime);
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

}
