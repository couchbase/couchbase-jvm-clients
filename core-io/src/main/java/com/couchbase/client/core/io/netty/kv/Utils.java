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

import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.util.Attribute;

/**
 * Provides various helper utilities for forking with a KV channel.
 *
 * @since 2.0.0
 */
public enum Utils {
  ;

  /**
   * Allows to access or modify the opaque value of a channel.
   *
   * *NOTE*: this is not thread safe and only allowed to be called inside
   * a channel!
   *
   * @param channel the channel to work with.
   * @param increment if it should be incremented and stored before returning.
   * @return the opaque value, either incremented or not.
   */
  static int opaque(final Channel channel, final boolean increment) {
    Attribute<Integer> attr = channel.attr(ChannelAttributes.OPAQUE_KEY);
    Integer value = attr.get();
    if (value == null) {
      value = 0;
      attr.set(value);
    }
    if (increment) {
      attr.set(++value);
    }
    return value;
  }

}
