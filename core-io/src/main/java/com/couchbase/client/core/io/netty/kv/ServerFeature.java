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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.EnumLookupTable;

/**
 * The {@link ServerFeature} enum describes all the different negotiation modes
 * between the server and the SDK.
 *
 * @since 2.0.0
 */
@Stability.Internal
public enum ServerFeature {

  /**
   * The custom datatype feature.
   *
   * @since Couchbase Server 4.0
   * @deprecated this feature is considered retired.
   */
  @Deprecated
  DATATYPE((short) 0x01),

  /**
   * The TLS feature.
   */
  TLS((byte) 0x02),

  /**
   * Enables TCP Nodelay.
   *
   * @since Couchbase Server 4.0
   */
  TCPNODELAY((short) 0x03),

  /**
   * Returns the sequence number on every mutation.
   *
   * @since Couchbase Server 4.0
   */
  MUTATION_SEQNO((short) 0x04),

  /**
   * Disable TCP Nodelay.
   *
   * @since Couchbase Server 4.0
   */
  TCPDELAY((short) 0x05),

  /**
   * Enable xattr support.
   *
   * @since Couchbase Server Spock (5.0)
   */
  XATTR((short) 0x06),

  /**
   * Enable extended error map support.
   *
   * @since Couchbase Server Spock (5.0)
   */
  XERROR((short) 0x07),

  /**
   * Enable select_bucket support.
   *
   * @since Couchbase Server Spock (5.0)
   */
  SELECT_BUCKET((short) 0x08),

  /**
   * Enable snappy-based compression support.
   *
   * @since Couchbase Server Vulcan (5.5)
   */
  SNAPPY((short) 0x0a),

  /**
   * Enables JSON data identification support.
   *
   * @since Couchbase Server Vulcan (5.5)
   */
  JSON((short) 0x0b),

  /**
   * Enables Duplex mode support.
   */
  DUPLEX((short) 0x0c),

  /**
   * Request the server to push any cluster maps stored by ns_server into
   * one of the buckets the client have access to.
   */
  CLUSTERMAP_CHANGE_NOTIFICATION((short) 0x0d),

  /**
   * Tell the server that we're ok with the server reordering the execution
   * of commands.
   */
  UNORDERED_EXECUTION((short) 0x0e),

  /**
   * Enable tracing support.
   *
   * @since Couchbase Server Vulcan (5.5)
   */
  TRACING((short) 0x0f),

  /**
   * Allows the server to accept requests with flexible extras.
   */
  ALT_REQUEST((short) 0x10),

  /**
   * Specify durability requirements for mutations.
   */
  SYNC_REPLICATION((short) 0x11),

  /**
   * Enables the collections feature.
   *
   * @since Couchbase Server Spock (5.0)
   */
  COLLECTIONS((short) 0x12),

  /**
   * Enables preserving expiry when updating document.
   *
   * @since Couchbase Server 7.0
   */
  PRESERVE_TTL((short) 0x14),

  /**
   * Enables the vattr feature.
   *
   * Note that vattrs (such as $document) were available before this, but this flag signifies that if a vattr is
   * requested that the server does not recognise, it will be rejected with the correct XATTR_UNKNOWN_VATTR error,
   * rather than the connection being disconnected.
   *
   * @since Couchbase Server 6.5.1
   */
  VATTR((short) 0x15),

  /**
   * Enables the "create as deleted" flag, allowing a document to be created in a tombstoned state.
   *
   * @since Couchbase Server 6.6
   */
  CREATE_AS_DELETED((short) 0x17),
  ;

  private static final EnumLookupTable<ServerFeature> lookupTable =
      EnumLookupTable.create(ServerFeature.class, e -> Short.toUnsignedInt(e.value()));

  /**
   * The actual byte representation on the wire.
   */
  private final short value;

  /**
   * Creates a new server feature.
   *
   * @param value the hex value from the wire protocol.
   */
  ServerFeature(short value) {
    this.value = value;
  }

  /**
   * Returns the actual byte value for the wire protocol.
   *
   * @return the actual wire value.
   */
  public short value() {
    return value;
  }

  /**
   * Returns the server feature associated with the raw short value, or null if not found.
   */
  static ServerFeature from(final short input) {
    return lookupTable.getOrDefault(Short.toUnsignedInt(input), null);
  }
}
