package com.couchbase.client.core.env;

public class RoleBasedCredentials implements Credentials {

  private final String username;
  private final String password;

  public RoleBasedCredentials(String username, String password) {
    this.username = username;
    this.password = password;
  }

  @Override
  public String usernameForBucket(String bucket) {
    return username;
  }

  @Override
  public String passwordForBucket(String bucket) {
    return password;
  }

}
