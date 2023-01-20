package com.couchbase.client.java;

public abstract class CommonDropOptions<DSELF extends CommonDropOptions<DSELF>> extends CommonOptions<DSELF> {
  private boolean ignoreIfNotExists;
  /**
   * If the bucket exists, an exception will be thrown unless this is set to true.
   */
  public CommonDropOptions<DSELF> ignoreIfNotExists(boolean ignore) {
    this.ignoreIfNotExists = ignore;
    return this;
  }

  protected abstract class BuiltDropOptions extends BuiltCommonOptions  {

    public boolean ignoreIfNotExists() {
      return ignoreIfNotExists;
    }
  }
}
