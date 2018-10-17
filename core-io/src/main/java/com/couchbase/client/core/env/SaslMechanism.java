package com.couchbase.client.core.env;

public enum SaslMechanism {
  PLAIN("PLAIN"),
  CRAM_MD5("CRAM-MD5"),
  SCRAM_SHA1("SCRAM-SHA1"),
  SCRAM_SHA256("SCRAM-SHA256"),
  SCRAM_SHA512("SCRAM-SHA512");

  private final String mech;

  SaslMechanism(String mech) {
    this.mech = mech;
  }

  public String mech() {
    return mech;
  }

  /**
   * Helper method to create the enum from its string representation.
   *
   * @param mech the mechanism to convert.
   * @return null if not found, otherwise the enum representation.
   */
  public static SaslMechanism from(final String mech) {
    if (mech.equalsIgnoreCase("PLAIN")) {
      return PLAIN;
    } else if (mech.equalsIgnoreCase("CRAM-MD5")) {
      return CRAM_MD5;
    } else if (mech.equalsIgnoreCase("SCRAM-SHA1")) {
      return SCRAM_SHA1;
    } else if (mech.equalsIgnoreCase("SCRAM-SHA256")) {
      return SCRAM_SHA256;
    } else if (mech.equalsIgnoreCase("SCRAM-SHA512")) {
      return SCRAM_SHA512;
    }

    return null;
  }
}
