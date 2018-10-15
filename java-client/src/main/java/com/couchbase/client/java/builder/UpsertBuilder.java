package com.couchbase.client.java.builder;

import com.couchbase.client.java.Document;
import com.couchbase.client.java.PersistTo;

import java.time.Duration;

public class UpsertBuilder extends AbstractBuilder<Document, UpsertBuilder> {

  private PersistTo persistence;

  public UpsertBuilder(Duration timeout) {
    super(timeout);
  }

  public UpsertBuilder withPersistence(final PersistTo persistence) {
    this.persistence = persistence;
    return this;
  }


}
