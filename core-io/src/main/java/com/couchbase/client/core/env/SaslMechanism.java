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

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Describes the support SASL authentication mechanisms.
 */
public enum SaslMechanism {

  PLAIN("PLAIN", 1),
  SCRAM_SHA1("SCRAM-SHA1", 2),
  SCRAM_SHA256("SCRAM-SHA256", 2),
  SCRAM_SHA512("SCRAM-SHA512", 2),
  OAUTHBEARER("OAUTHBEARER", 1),
  ;

  private final String mech;
  private final int roundtrips;

  private static final Map<String, SaslMechanism> lookupTable = unmodifiableMap(
    Arrays.stream(SaslMechanism.values())
      .collect(toMap(it -> it.mech, it -> it))
  );

  SaslMechanism(String mech, int roundtrips) {
    this.mech = requireNonNull(mech);
    this.roundtrips = roundtrips;
  }

  /**
   * Returns the "raw" string representation of the mechanism on the wire.
   */
  public String mech() {
    return mech;
  }

  /**
   * Returns the number of roundtrips this algorithm has with the server.
   */
  public int roundtrips() {
    return roundtrips;
  }

  /**
   * Helper method to create the enum from its string representation.
   *
   * @param mech the mechanism to convert.
   * @return null if not found, otherwise the enum representation.
   */
  public static @Nullable SaslMechanism from(final String mech) {
    return lookupTable.get(mech);
  }
}
