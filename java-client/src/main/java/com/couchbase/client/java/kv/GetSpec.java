package com.couchbase.client.java.kv;

public class GetSpec {

  public static GetSpec FULL_DOC = new GetSpec();

  private GetSpec() {

  }

  public static GetSpec getSpec() {
    return new GetSpec();
  }


  public GetSpec getFields(String... paths) {
    for (String p : paths) {
      getField(p);
    }
    return this;
  }

  public GetSpec countFields(String... paths) {
    for (String p : paths) {
      countField(p);
    }
    return this;
  }

  public GetSpec checkFields(String... paths) {
    for (String p : paths) {
      checkField(p);
    }
    return this;
  }

  public GetSpec getField(String path, FieldOption... options) {
    return this;
  }

  public GetSpec countField(String path, FieldOption... options) {
    return this;
  }

  public GetSpec checkField(String path, FieldOption... options) {
    return this;
  }

  public enum FieldOption {
    XATTR,
    ACCESS_DELETED,
  }

}
