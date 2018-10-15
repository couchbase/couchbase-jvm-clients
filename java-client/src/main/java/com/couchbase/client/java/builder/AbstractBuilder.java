package com.couchbase.client.java.builder;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class AbstractBuilder<T, SELF extends AbstractBuilder<T, SELF>> {

  private Duration timeout;

  protected AbstractBuilder(Duration timeout) {
    this.timeout = timeout;
  }

  public SELF withTimeout(Duration timeout) {
    this.timeout = timeout;
    return self();
  }

  public T execute() {
    try {
      return executeAsync().get();
    } catch (Exception ex) {
      // handle properly?
      return null;
    }
  }

  public CompletableFuture<T> executeAsync() {
    return null;
  }

  protected Duration timeout() {
    return timeout;
  }

  @SuppressWarnings({ "unchecked" })
  protected SELF self() {
    return (SELF) this;
  }

}
