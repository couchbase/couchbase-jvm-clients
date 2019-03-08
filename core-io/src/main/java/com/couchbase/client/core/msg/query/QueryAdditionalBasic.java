package com.couchbase.client.core.msg.query;

import java.util.List;
import java.util.Optional;

/**
 * Store any data received after the results & errors from a N1QL query.
 *
 * The bindings are expected to convert this low-level class into something more user-friendly.
 */
public class QueryAdditionalBasic {
  // All parameters are final for this simple value class, so no private or getters/setters required

  public final List<byte[]> warnings;
  public final Optional<byte[]> profile;
  public final byte[] metrics;
  public final String status;

  public QueryAdditionalBasic(List<byte[]> warnings, Optional<byte[]> profile, byte[] metrics, String status) {
    this.warnings = warnings;
    this.profile = profile;
    this.metrics = metrics;
    this.status = status;
  }
}
