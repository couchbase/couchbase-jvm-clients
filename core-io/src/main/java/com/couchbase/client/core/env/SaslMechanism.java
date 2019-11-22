/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

public enum SaslMechanism {
  PLAIN("PLAIN"),
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
