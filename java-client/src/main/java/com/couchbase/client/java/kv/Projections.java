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

  private Projections add(SubdocGetRequest.CommandType type, boolean xattr, String... paths) {
    for (String path : paths) {
      commands.add(new SubdocGetRequest.Command(type, path, xattr));
    }
    return this;
  }

  public Projections get(String... paths) {
    return add(SubdocGetRequest.CommandType.GET, false, paths);
  }

  public Projections exists(String... paths) {
    return add(SubdocGetRequest.CommandType.EXISTS, false, paths);
  }

  public Projections count(String... paths) {
    return add(SubdocGetRequest.CommandType.COUNT, false, paths);
  }

  public Projections getXAttr(String... paths) {
    return add(SubdocGetRequest.CommandType.GET, true, paths);
  }

  public Projections existsXAttr(String... paths) {
    return add(SubdocGetRequest.CommandType.EXISTS, true, paths);
  }

  public Projections countXAttr(String... paths) {
    return add(SubdocGetRequest.CommandType.COUNT, true, paths);
  }

  @Stability.Internal
  public List<SubdocGetRequest.Command> commands() {
    return commands;
  }


}
