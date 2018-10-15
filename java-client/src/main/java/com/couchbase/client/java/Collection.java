package com.couchbase.client.java;

import com.couchbase.client.java.builder.GetBuilder;
import com.couchbase.client.java.builder.UpsertBuilder;

public interface Collection {

  GetBuilder get(String id);

  UpsertBuilder upsert(Document document);

}
