package com.couchbase.client.core.env;

public class RoleBasedCredentials implements Credentials {

  private final String username;
  private final String password;

  public RoleBasedCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String username() {
    return username;
  }

  public String password() {
    return password;
  }
}
