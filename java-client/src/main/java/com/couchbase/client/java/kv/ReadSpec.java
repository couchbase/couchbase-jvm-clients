package com.couchbase.client.java.kv;

public class ReadSpec {

  public static ReadSpec FULL_DOC = new ReadSpec();

  private ReadSpec() {

  }

  public static ReadSpec readSpec() {
    return new ReadSpec();
  }


  public ReadSpec getFields(String... paths) {
    for (String p : paths) {
      getField(p);
    }
    return this;
  }

  public ReadSpec countFields(String... paths) {
    for (String p : paths) {
      countField(p);
    }
    return this;
  }

  public ReadSpec checkFields(String... paths) {
    for (String p : paths) {
      checkField(p);
    }
    return this;
  }

  public ReadSpec getField(String path, FieldOption... options) {
    return this;
  }

  public ReadSpec countField(String path, FieldOption... options) {
    return this;
  }

  public ReadSpec checkField(String path, FieldOption... options) {
    return this;
  }

  public enum FieldOption {
    XATTR,
    ACCESS_DELETED,
  }

}
