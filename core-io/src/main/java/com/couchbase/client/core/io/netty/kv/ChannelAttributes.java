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

import com.couchbase.client.core.deps.io.netty.util.AttributeKey;
import com.couchbase.client.core.env.SaslMechanism;

import java.util.List;
import java.util.Set;

/**
 * This class holds all kinds of channel attributes that are in used across
 * the KV channel.
 *
 * @since 2.0.0
 */
public class ChannelAttributes {

  private ChannelAttributes() {}

  /**
   * Holds the negotiated server features in a channel.
   */
  static final AttributeKey<Set<ServerFeature>> SERVER_FEATURE_KEY =
    AttributeKey.newInstance("ServerFeatures");

  /**
   * Holds the connect timings for a channel.
   */
  static final AttributeKey<ConnectTimings> CONNECT_TIMINGS_KEY =
    AttributeKey.newInstance("ConnectTimings");

  /**
   * Holds the potentially loaded error map in a channel.
   */
  static final AttributeKey<ErrorMap> ERROR_MAP_KEY =
    AttributeKey.newInstance("ErrorMap");

  /**
   * Holds the channel ID negotiated with KV engine once set.
   */
  public static final AttributeKey<String> CHANNEL_ID_KEY =
    AttributeKey.newInstance("ChannelId");

  /**
   * Holds the SASL mechanisms the server supports.
   */
  public static final AttributeKey<Set<SaslMechanism>> SASL_MECHS_KEY =
    AttributeKey.newInstance("SaslMechs");

  /**
   * Holds the UUID of the bucket selected on this channel, if it could be fetched during the handshake.
   * <p>
   * The UUID comes from the bucket config that {@link SelectBucketHandler} requests right after selecting
   * the bucket. It's absent when the server has no config to return, or the config can't be used:
   * <ul>
   *   <li>Memcached buckets. The cluster manager never gives KV a config for them.</li>
   *   <li>A bucket that was just created. KV can let clients select the bucket before the cluster manager
   *   gives it the bucket's config. Since Couchbase Server 7.6.2, the cluster manager waits until the bucket
   *   has a vbucket map, which can take a fraction of a second (MB-60405).</li>
   *   <li>Servers that don't return a bucket config this way, or return one without a UUID, such as
   *   some mock servers.</li>
   *   <li>A config the client can't parse.</li>
   * </ul>
   * A channel without a UUID doesn't know which incarnation of the bucket it's talking to, so it can't share
   * cached collection IDs with other channels. It keeps its own instead, for as long as it's open.
   * <p>
   * If the SDK drops memcached bucket support, a {@code KEY_ENOENT} (or a config without a {@code uuid}) from
   * the handshake's {@code GET_CONFIG} would only mean "not ready yet". {@link SelectBucketHandler} could then
   * fail the connect with a retryable error, so every bucket connection knows its incarnation and the private
   * collection ID cache could go.
   */
  static final AttributeKey<String> BUCKET_UUID_KEY =
    AttributeKey.newInstance("BucketUuid");

}
