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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.EnumLookupTable;

/**
 * Describes all the different negotiation modes
 * between the server and the SDK.
 *
 * @since 2.0.0
 */
@Stability.Internal
public enum ServerFeature {

  /**
   * The custom datatype feature.
   *
   * @deprecated this feature is considered retired.
   */
  @SinceCouchbase("4.0")
  @Deprecated
  DATATYPE(0x01),

  /**
   * The TLS feature.
   */
  TLS((byte) 0x02),

  /**
   * Enables TCP Nodelay.
   */
  @SinceCouchbase("4.0")
  TCPNODELAY(0x03),

  /**
   * Returns the sequence number on every mutation.
   */
  @SinceCouchbase("4.0")
  MUTATION_SEQNO(0x04),

  /**
   * Disable TCP Nodelay.
   */
  @SinceCouchbase("4.0")
  TCPDELAY(0x05),

  /**
   * Enable xattr support.
   */
  @SinceCouchbase("5.0")
  XATTR(0x06),

  /**
   * Enable extended error map support.
   */
  @SinceCouchbase("5.0")
  XERROR(0x07),

  /**
   * Enable select_bucket support.
   */
  @SinceCouchbase("5.0")
  SELECT_BUCKET(0x08),

  /**
   * Enable snappy-based compression support.
   */
  @SinceCouchbase("5.5")
  SNAPPY(0x0a),

  /**
   * Enables JSON data identification support.
   */
  @SinceCouchbase("5.5")
  JSON(0x0b),

  /**
   * Enables Duplex mode support.
   */
  @SinceCouchbase("5.5")
  DUPLEX(0x0c),

  /**
   * Request the server to push any cluster maps stored by ns_server into
   * one of the buckets the client have access to.
   */
  @SinceCouchbase("5.5")
  CLUSTERMAP_CHANGE_NOTIFICATION(0x0d),

  /**
   * Tell the server that we're ok with the server reordering the execution
   * of commands.
   */
  @SinceCouchbase("5.5")
  UNORDERED_EXECUTION(0x0e),

  /**
   * Enable tracing support.
   */
  @SinceCouchbase("5.5")
  TRACING(0x0f),

  /**
   * Allows the server to accept requests with flexible extras.
   */
  @SinceCouchbase("6.5")
  ALT_REQUEST(0x10),

  /**
   * Specify durability requirements for mutations.
   */
  @SinceCouchbase("6.5")
  SYNC_REPLICATION(0x11),

  /**
   * Enables the collections feature.
   * <p>
   * History note: There was a "collections" feature in Couchbase in 5.0,
   * but it had a different code (0x09) that has since been retired.
   */
  @SinceCouchbase("7.0")
  COLLECTIONS(0x12),

  /**
   * Enables preserving expiry when updating document.
   */
  @SinceCouchbase("7.0")
  PRESERVE_TTL(0x14),

  /**
   * Enables the vattr feature.
   * <p>
   * Note that vattrs (such as $document) were available before this, but this flag signifies that if a vattr is
   * requested that the server does not recognise, it will be rejected with the correct XATTR_UNKNOWN_VATTR error,
   * rather than the connection being disconnected.
   */
  @SinceCouchbase("6.5.1")
  VATTR(0x15),

  /**
   * Enables the "create as deleted" flag, allowing a document to be created in a tombstoned state.
   */
  @SinceCouchbase("6.6")
  CREATE_AS_DELETED(0x17),

  /**
   * When enabled, the server will insert frame info field(s) in the response
   * containing the amount of read and write units the command used on the
   * server, and the time the command spent throttled on the server. The
   * fields will only be inserted if non-zero.
   */
  @SinceCouchbase("7.2")
  REPORT_UNIT_USAGE(0x1a),
  ;

  private static final EnumLookupTable<ServerFeature> lookupTable =
    EnumLookupTable.create(ServerFeature.class, ServerFeature::value);

  /**
   * The actual byte representation on the wire.
   */
  private final int value;

  /**
   * Creates a new server feature.
   *
   * @param value the hex value from the wire protocol.
   */
  ServerFeature(int value) {
    this.value = require16Bit(value);
  }

  /**
   * Returns an int whose low 16 bits contain the feature's value in the wire protocol.
   *
   * @return the actual wire value.
   */
  public int value() {
    return value;
  }

  private static int require16Bit(int value) {
    if ((value & 0xffff) != value) {
      throw new IllegalArgumentException("Expected a value that fits in 16 bits, but got: 0x" + Integer.toHexString(value));
    }
    return value;
  }

  /**
   * Returns the server feature associated with the raw short value, or null if not found.
   */
  static ServerFeature from(final int input) {
    return lookupTable.getOrDefault(input, null);
  }
}
