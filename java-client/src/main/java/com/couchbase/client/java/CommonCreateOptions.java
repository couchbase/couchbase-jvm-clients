package com.couchbase.client.java;

public abstract class CommonCreateOptions<CSELF extends CommonCreateOptions<CSELF>> extends CommonOptions<CSELF> {
  private boolean ignoreIfExists;
  /**
   * If the bucket exists, an exception will be thrown unless this is set to true.
   */
  public CommonCreateOptions<CSELF> ignoreIfExists(boolean ignore) {
    this.ignoreIfExists = ignore;
    return this;
  }

  protected abstract class BuildCreateOptions extends BuiltCommonOptions  {

    public boolean ignoreIfExists() {
      return ignoreIfExists;
    }
  }
}
