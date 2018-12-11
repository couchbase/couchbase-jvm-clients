package com.couchbase.client.java.kv;

import java.util.HashMap;
import java.util.Map;

public class Projections {

  private final Map<String, ProjectionType> projections;

  public static Projections projections() {
    return new Projections();
  }

  private Projections() {
    projections = new HashMap<>();
  }

  private Projections add(ProjectionType type, String... paths) {
    for (String path : paths) {
      projections.put(path, type);
    }
    return this;
  }

  public Projections get(String... paths) {
    return add(ProjectionType.GET, paths);
  }

  public Projections exists(String... paths) {
    return add(ProjectionType.EXISTS, paths);
  }

  public Projections count(String... paths) {
    return add(ProjectionType.COUNT, paths);
  }

  enum ProjectionType {
    GET,
    EXISTS,
    COUNT
  }
}
