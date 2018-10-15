package com.couchbase.client.java.builder;

import com.couchbase.client.java.Document;

import java.time.Duration;

public class GetBuilder extends AbstractBuilder<Document, GetBuilder> {

  public GetBuilder(Duration timeout) {
    super(timeout);
  }


}
