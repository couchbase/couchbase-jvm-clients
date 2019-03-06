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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;

/**
 * Interface which provides a common API to initialize endpoint-specific pipelines.
 *
 * @since 2.0.0
 */
public interface PipelineInitializer {

  /**
   * Initializes the pipeline with the handlers that are part of the implementation.
   *
   * @param pipeline the pipeline to modify.
   */
  void init(final ChannelPipeline pipeline);

}
