package de.hpi.des.hdes.benchmark.nexmark.entities;

import java.util.Objects;

public final class Person {

  public long id;
  public String name;
  public String email;
  public String phone;
  public String homepage;
  public String creditcard;

  // Address
  public String street;
  public String city;
  public String province;
  public String country;
  public String zipCode;

  // Profile
  public String education;
  public String gender;
  public String business;
  public String age;
  public String income;

  public Long eventTime;

  public Person() {}

  public Person(long id, String name, String email, String phone, String homepage, String creditcard, String street,
                String city, String province, String country, String zipCode, String education, String gender,
                String business, String age, String income, long eventTime) {
    this.id = id;
    this.name = name;
    this.email = email;
    this.phone = phone;
    this.homepage = homepage;
    this.creditcard = creditcard;
    this.street = street;
    this.city = city;
    this.province = province;
    this.country = country;
    this.zipCode = zipCode;
    this.education = education;
    this.gender = gender;
    this.business = business;
    this.age = age;
    this.income = income;
    this.eventTime = eventTime;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getPhone() {
    return phone;
  }

  public void setPhone(String phone) {
    this.phone = phone;
  }

  public String getHomepage() {
    return homepage;
  }

  public void setHomepage(String homepage) {
    this.homepage = homepage;
  }

  public String getCreditcard() {
    return creditcard;
  }

  public void setCreditcard(String creditcard) {
    this.creditcard = creditcard;
  }

  public String getStreet() {
    return street;
  }

  public void setStreet(String street) {
    this.street = street;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getProvince() {
    return province;
  }

  public void setProvince(String province) {
    this.province = province;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }

  public String getEducation() {
    return education;
  }

  public void setEducation(String education) {
    this.education = education;
  }

  public String getGender() {
    return gender;
  }

  public void setGender(String gender) {
    this.gender = gender;
  }

  public String getBusiness() {
    return business;
  }

  public void setBusiness(String business) {
    this.business = business;
  }

  public String getAge() {
    return age;
  }

  public void setAge(String age) {
    this.age = age;
  }

  public String getIncome() {
    return income;
  }

  public void setIncome(String income) {
    this.income = income;
  }

  public Long getEventTime() {
    return eventTime;
  }

  public void setEventTime(Long eventTime) {
    this.eventTime = eventTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Person person = (Person) o;
    return id == person.id &&
            Objects.equals(name, person.name) &&
            Objects.equals(email, person.email) &&
            Objects.equals(phone, person.phone) &&
            Objects.equals(homepage, person.homepage) &&
            Objects.equals(creditcard, person.creditcard) &&
            Objects.equals(street, person.street) &&
            Objects.equals(city, person.city) &&
            Objects.equals(province, person.province) &&
            Objects.equals(country, person.country) &&
            Objects.equals(zipCode, person.zipCode) &&
            Objects.equals(education, person.education) &&
            Objects.equals(gender, person.gender) &&
            Objects.equals(business, person.business) &&
            Objects.equals(age, person.age) &&
            Objects.equals(income, person.income);
  }

  @Override
  public String toString() {
    return "Person{" +
        "id=" + id +
        ", name='" + name + '\'' +
        ", email='" + email + '\'' +
        ", phone='" + phone + '\'' +
        ", homepage='" + homepage + '\'' +
        ", creditcard='" + creditcard + '\'' +
        ", street='" + street + '\'' +
        ", city='" + city + '\'' +
        ", province='" + province + '\'' +
        ", country='" + country + '\'' +
        ", zipCode='" + zipCode + '\'' +
        ", education='" + education + '\'' +
        ", gender='" + gender + '\'' +
        ", business='" + business + '\'' +
        ", age='" + age + '\'' +
        ", income='" + income + '\'' +
        ", eventTime=" + eventTime +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(id, name, email, phone, homepage, creditcard, street, city, province, country,
            zipCode, education, gender, business, age, income, eventTime);
  }
}
