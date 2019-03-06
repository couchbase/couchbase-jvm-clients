package com.couchbase.client.java.examples.kv;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonGetter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Person {
  private final String name;
  private final int age;
  private final List<String> animals;
  private final Attributes attributes;
  private final String type = "person";

  @JsonCreator
  Person(
    @JsonProperty("name") String name,
    @JsonProperty("age") int age,
    @JsonProperty("animals") List<String> animals,
    @JsonProperty("attributes") Attributes attributes) {
    this.name = name;
    this.age = age;
    this.animals = animals;
    this.attributes = attributes;
  }

  @JsonGetter
  public String name() {
    return name;
  }

  @JsonGetter
  public int age() {
    return age;
  }

  @JsonGetter
  public List<String> animals() {
    return animals;
  }

  @JsonGetter
  public Attributes attributes() {
    return attributes;
  }

  @JsonGetter
  public String type() {
    return type;
  }

  static class Dimensions {

    private final int height;
    private final int weight;

    @JsonCreator
    Dimensions(@JsonProperty("height") int height, @JsonProperty("weight") int weight) {
      this.height = height;
      this.weight = weight;
    }

    @JsonGetter
    public int height() {
      return height;
    }

    @JsonGetter
    public int weight() {
      return weight;
    }
  }

  static class Location {

    private final double lat;
    private final double lon;

    @JsonCreator
    Location(@JsonProperty("lat") double lat, @JsonProperty("lon") double lon) {
      this.lat = lat;
      this.lon = lon;
    }

    @JsonGetter
    public double lat() {
      return lat;
    }

    @JsonGetter
    public double lon() {
      return lon;
    }
  }

  static class Details {

    private final Location location;

    @JsonCreator
    Details(@JsonProperty("location") Location location) {
      this.location = location;
    }

    @JsonGetter
    public Location location() {
      return location;
    }
  }

  static class Hobby {
    private final String type;
    private final String name;
    private final Details details;

    @JsonCreator
    Hobby(
      @JsonProperty("type") String type,
      @JsonProperty("name") String name,
      @JsonProperty("details") Details details) {
      this.type = type;
      this.name = name;
      this.details = details;
    }

    @JsonGetter
    public String type() {
      return type;
    }

    @JsonGetter
    public String name() {
      return name;
    }

    @JsonGetter
    public Details details() {
      return details;
    }
  }

  static class Attributes {
    private final String hair;
    private final Dimensions dimensions;
    private final List<Hobby> hobbies;

    @JsonCreator
    Attributes(
      @JsonProperty("hair") String hair,
      @JsonProperty("dimensions") Dimensions dimensions,
      @JsonProperty("hobbies") List<Hobby> hobbies) {
      this.hair = hair;
      this.dimensions = dimensions;
      this.hobbies = hobbies;
    }

    @JsonGetter
    public String hair() {
      return hair;
    }

    @JsonGetter
    public Dimensions dimensions() {
      return dimensions;
    }

    @JsonGetter
    public List<Hobby> hobbies() {
      return hobbies;
    }
  }
}