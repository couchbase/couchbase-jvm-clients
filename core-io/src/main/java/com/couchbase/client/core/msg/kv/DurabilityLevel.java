package com.couchbase.client.core.msg.kv;

public enum DurabilityLevel {
  MAJORITY((byte) 0x01),
  MAJORITY_AND_PERSIST_ON_MASTER((byte) 0x02),
  PERSIST_TO_MAJORITY((byte) 0x03);

  private final byte code;

  DurabilityLevel(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }
}
