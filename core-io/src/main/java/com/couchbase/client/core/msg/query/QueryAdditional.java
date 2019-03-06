package com.couchbase.client.core.msg.query;

public class QueryAdditional {
  // All parameters are final for this simple value class, so no private or getters/setters required

  public final byte[] warnings;
  public final byte[] profile;
  public final byte[] metrics;
  public final String status;

  public QueryAdditional(byte[] warnings, byte[] profile, byte[] metrics, String status) {
    this.warnings = warnings;
    this.profile = profile;
    this.metrics = metrics;
    this.status = status;
  }
}
