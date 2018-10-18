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

import io.netty.util.AttributeKey;

import java.util.List;

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
  static final AttributeKey<List<ServerFeature>> SERVER_FEATURE_KEY =
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
   * Holds the opaques used for this channel.
   */
  static final AttributeKey<Integer> OPAQUE_KEY =
    AttributeKey.newInstance("Opaque");

}
