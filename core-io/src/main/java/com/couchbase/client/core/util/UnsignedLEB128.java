package com.couchbase.client.core.util;

import java.util.ArrayList;
import java.util.List;

public class UnsignedLEB128 {
  /**
   * Encode the given integer to LEB128 byte array
   *
   * @param in the number to be encoded
   * @return encoded byte array
   */
  public static byte[] encode(long in) {
    List<Byte> encoded = new ArrayList<>();
    encoded.add((byte) (in & 0x7F));
    in = in>>>7;

    while(in != 0) {
      encoded.add((byte)(in & 0x7F));
      in = in>>>7;
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
  public static long decode(byte[] in) {
    long res = 0;
    for (int i=in.length-1,j=0; i>=0; i--,j++) {
      res = res | ((in[i] & 0x7F) << (7 * j));
    }
    return res;
  }

}