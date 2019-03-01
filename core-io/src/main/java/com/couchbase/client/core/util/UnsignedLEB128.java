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

package com.couchbase.client.core.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Unsigned LEB 128 converter util from big integer
 */
public class UnsignedLEB128 {
  private static BigInteger x = new BigInteger("7F", 16);

  /**
   * Encode the given integer to LEB128 byte array
   *
   * @param in the number to be encoded
   * @return encoded byte array
   */
  public static byte[] encode(BigInteger in) {
    List<Byte> encoded = new ArrayList<>();
    encoded.add(in.and(x).toByteArray()[0]);
    in = in.shiftRight(7);

    while(in.bitCount() > 0) {
      encoded.add(in.and(x).toByteArray()[0]);
      in = in.shiftRight(7);
    }

    byte[] res = new byte[encoded.size()];
    for(int j=encoded.size()-1,i=0;j>=0;j--,i++) {
      res[i] = i == 0 ? encoded.get(j) : ((byte)(encoded.get(j) | 0x80));
    }
    return res;
  }

  /**
   * Decode the given LEB128 byte array to int
   *
   * @param in byte array
   * @return decoded integer
   */
  public static BigInteger decode(byte[] in) {
    BigInteger res = BigInteger.valueOf(0);
    for (int i=in.length-1,j=0; i>=0; i--,j++) {
      BigInteger t = BigInteger.valueOf(in[i]);
      res = res.or(t.and(x).shiftLeft(7 * j));
    }
    return res;
  }
}