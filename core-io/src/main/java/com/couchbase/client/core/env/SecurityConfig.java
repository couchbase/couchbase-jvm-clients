package com.couchbase.client.core.env;

public class SecurityConfig {

  private boolean certAuthEnabled;

  public SecurityConfig(boolean certAuthEnabled) {
    this.certAuthEnabled = certAuthEnabled;
  }

  public boolean certAuthEnabled() {
    return certAuthEnabled;
  }
}
