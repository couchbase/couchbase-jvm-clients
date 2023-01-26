/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.annotation.Stability;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

// JVMCBC-1187: This and everything using it will be removed before GA.
@Stability.Internal
public class ProtostellarStatsCollector {
  private int channelsCreated = 0;
  private int subChannelsCreated = 0;

  private AtomicInteger concurrentlyOutgoingMessages = new AtomicInteger();
  private Set<Integer> concurrentlyOutgoingMessagesSeen = new ConcurrentSkipListSet<>();
  private AtomicInteger concurrentlyIncomingMessages = new AtomicInteger();
  private Set<Integer> concurrentlyIncomingMessagesSeen = new ConcurrentSkipListSet<>();
  private Set<Integer> maxThreadCountsSeen = new ConcurrentSkipListSet<>();

  public void channelAdded() {
    channelsCreated++;
  }

  public void subChannelAdded() {
    subChannelsCreated++;
  }

  public int channelsCreated() {
    return channelsCreated;
  }

  public int subChannelsCreated() {
    return subChannelsCreated;
  }

  public void reset() {
    this.channelsCreated = 0;
    this.subChannelsCreated = 0;
  }

  public void outboundMessageSent() {
    concurrentlyOutgoingMessages.decrementAndGet();
  }

  public void outboundMessage() {
    concurrentlyOutgoingMessagesSeen.add(concurrentlyOutgoingMessages.incrementAndGet());
  }

  public void inboundMessage() {
    concurrentlyIncomingMessagesSeen.add(concurrentlyIncomingMessages.incrementAndGet());
  }

  public void inboundMessageRead() {
    concurrentlyIncomingMessages.decrementAndGet();
  }

  public int inflightOutgoingMessagesMax() {
    return concurrentlyOutgoingMessagesSeen
      .stream()
      .max(Comparator.comparingInt(v -> v))
      .orElse(0);
  }

  public int inflightIncomingMessagesMax() {
    return concurrentlyIncomingMessagesSeen
      .stream()
      .max(Comparator.comparingInt(v -> v))
      .orElse(0);
  }

  public void currentMaxThreadCount(int activeThreadCount) {
    maxThreadCountsSeen.add(activeThreadCount);
  }

  public int maxThreadCount() {
    return maxThreadCountsSeen.stream()
      .max(Comparator.comparingInt(v -> v))
      .orElse(0);
  }
}
