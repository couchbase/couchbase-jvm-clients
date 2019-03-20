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

package com.couchbase.client.java.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EncodedJsonContent extends TypedContent<byte[]> {

  public static EncodedJsonContent wrap(String content) {
    return wrap(content.getBytes(UTF_8));
  }

  public static EncodedJsonContent wrap(byte[] content) {
    return new EncodedJsonContent(content);
  }

  private EncodedJsonContent(byte[] content) {
    super(content);
  }

  @Override
  public byte[] encoded() {
    return content();
  }

  @Override
  public int flags() {
    return Encoder.JSON_FLAGS;
  }


}
