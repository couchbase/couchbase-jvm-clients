package com.couchbase.client.java.kv;

import com.couchbase.client.java.CommonOptions;

import java.time.Duration;

public class GetAndLockOptions extends CommonOptions<GetAndLockOptions> {
  public static GetAndLockOptions DEFAULT = new GetAndLockOptions();

  private Duration lockFor;

  public GetAndLockOptions lockFor(Duration duration) {
    this.lockFor = duration;
    return this;
  }

  public Duration lockFor() {
    return lockFor;
  }


}
