package com.couchbase.client.java.kv;

import com.couchbase.client.core.msg.kv.MutationToken;

import java.util.Optional;

public class CounterResult extends MutationResult {

  private final long content;

  public CounterResult(long cas, long content, Optional<MutationToken> mutationToken) {
    super(cas, mutationToken);
    this.content = content;
  }

  public long content() {
    return content;
  }
}
