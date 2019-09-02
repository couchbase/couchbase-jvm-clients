/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.codec;

/**
 * The transcoder is responsible for transcoding KV binary packages between their binary and their java object
 * representation.
 */
public interface Transcoder {

  /**
   * Encodes the given input into the wire representation based on the data format.
   *
   * @param input the input object to encode.
   * @param format the data format which describes how to encode.
   * @return the encoded wire representation of the payload.
   */
  byte[] encode(Object input, DataFormat format);

  /**
   * Decodes the wire representation into the entity based on the data format.
   *
   * @param target the target type to decode.
   * @param input the wire representation to decode.
   * @param format the format which is used to decide the decoding serializer.
   * @return the decoded entity.
   */
  <T> T decode(Class<T> target, byte[] input, DataFormat format);

}
