package com.couchbase.client.core.env;

public class SecurityConfig {

  private boolean certAuthEnabled;

  public static SecurityConfig create() {
    return new SecurityConfig(false);
  }

  private SecurityConfig(boolean certAuthEnabled) {
    this.certAuthEnabled = certAuthEnabled;
  }

  public boolean certAuthEnabled() {
    return certAuthEnabled;
  }
}
