package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;

import java.util.ArrayList;
import java.util.List;

public class Projections {

  private final List<SubdocGetRequest.Command> commands;

  public static Projections projections() {
    return new Projections();
  }

  private Projections() {
    commands = new ArrayList<>();
  }

  private Projections add(SubdocGetRequest.CommandType type, String... paths) {
    for (String path : paths) {
      commands.add(new SubdocGetRequest.Command(type, path, false));
    }
    return this;
  }

  public Projections get(String... paths) {
    return add(SubdocGetRequest.CommandType.GET, paths);
  }

  public Projections exists(String... paths) {
    return add(SubdocGetRequest.CommandType.EXISTS, paths);
  }

  public Projections count(String... paths) {
    return add(SubdocGetRequest.CommandType.COUNT, paths);
  }

  @Stability.Internal
  public List<SubdocGetRequest.Command> commands() {
    return commands;
  }

}
