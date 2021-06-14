/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.error.CouchbaseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * This transcoder is compatible with the Java SDK 2 "LegacyTranscoder", which makes it usable
 * back to Java SDK 1 as a result.
 *
 * You would really only want to move to this transcoder if you already had to use the LegacyTranscoder in SDK 2
 * because you were using couchbase since the SDK 1 days (you must really love couchbase, thanks! :-)).
 */
public class Sdk2CompatibleLegacyTranscoder implements Transcoder {

  public static final Sdk2CompatibleLegacyTranscoder INSTANCE = new Sdk2CompatibleLegacyTranscoder();

  public static final int DEFAULT_COMPRESSION_THRESHOLD = 16384;

  private static final Pattern DECIMAL_PATTERN = Pattern.compile("^-?\\d+$");

  // General flags
  static final int SERIALIZED = 1;
  static final int COMPRESSED = 2;

  // Special flags for specially handled types.
  private static final int SPECIAL_MASK = 0xff00;
  static final int SPECIAL_BOOLEAN = (1 << 8);
  static final int SPECIAL_INT = (2 << 8);
  static final int SPECIAL_LONG = (3 << 8);
  static final int SPECIAL_DATE = (4 << 8);
  static final int SPECIAL_BYTE = (5 << 8);
  static final int SPECIAL_FLOAT = (6 << 8);
  static final int SPECIAL_DOUBLE = (7 << 8);
  static final int SPECIAL_BYTEARRAY = (8 << 8);

  private final int compressionThreshold;

  public Sdk2CompatibleLegacyTranscoder() {
    this(DEFAULT_COMPRESSION_THRESHOLD);
  }

  public Sdk2CompatibleLegacyTranscoder(int compressionThreshold) {
    this.compressionThreshold = compressionThreshold;
  }

  @Override
  public EncodedValue encode(final Object input) {
    int flags = 0;

    boolean isJson = false;
    byte[] encoded;
    if (input instanceof String) {
      String c = (String) input;
      isJson = isJsonObject(c);
      encoded = c.getBytes(StandardCharsets.UTF_8);
    } else {
      if (input instanceof Long) {
        flags |= SPECIAL_LONG;
        encoded = encodeNum((Long) input, 8);
      } else if (input instanceof Integer) {
        flags |= SPECIAL_INT;
        encoded = encodeNum((Integer) input, 4);
      } else if (input instanceof Boolean) {
        flags |= SPECIAL_BOOLEAN;
        boolean b = (Boolean) input;
        encoded = new byte[1];
        encoded[0] = (byte) (b ? '1' : '0');
      } else if (input instanceof Date) {
        flags |= SPECIAL_DATE;
        encoded = encodeNum(((Date) input).getTime(), 8);
      } else if (input instanceof Byte) {
        flags |= SPECIAL_BYTE;
        encoded = new byte[] { (Byte) input };
      } else if (input instanceof Float) {
        flags |= SPECIAL_FLOAT;
        encoded = encodeNum(Float.floatToRawIntBits((Float) input), 4);
      } else if (input instanceof Double) {
        flags |= SPECIAL_DOUBLE;
        encoded = encodeNum(Double.doubleToRawLongBits((Double) input), 8);
      } else if (input instanceof byte[]) {
        flags |= SPECIAL_BYTEARRAY;
        encoded = (byte[]) input;
      } else {
        flags |= SERIALIZED;
        encoded = serialize(input);
      }
    }

    if (!isJson && encoded.length >= compressionThreshold) {
      byte[] compressed = compress(encoded);
      if (compressed.length < encoded.length) {
        encoded = compressed;
        flags |= COMPRESSED;
      }
    }

    return new EncodedValue(encoded, flags);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T decode(final Class<T> target, byte[] input, final int flags) {
    Object decoded;
    if ((flags & COMPRESSED) != 0) {
      input = decompress(input);
    }
    int maskedFlags = flags & SPECIAL_MASK;
    if ((flags & SERIALIZED) != 0 && input != null) {
      decoded = deserialize(input);
    } else if (maskedFlags != 0 && input != null) {
      switch(maskedFlags) {
        case SPECIAL_BOOLEAN:
          decoded = input[0] == '1';
          break;
        case SPECIAL_INT:
          decoded = (int) decodeLong(input);
          break;
        case SPECIAL_LONG:
          decoded = decodeLong(input);
          break;
        case SPECIAL_DATE:
          decoded = new Date(decodeLong(input));
          break;
        case SPECIAL_BYTE:
          decoded = input[0];
          break;
        case SPECIAL_FLOAT:
          decoded = Float.intBitsToFloat((int) decodeLong(input));
          break;
        case SPECIAL_DOUBLE:
          decoded = Double.longBitsToDouble(decodeLong(input));
          break;
        case SPECIAL_BYTEARRAY:
          decoded = input;
          break;
        default:
          throw new CouchbaseException("Undecodeable with flags " + flags);
      }
    } else {
      decoded = new String(input, StandardCharsets.UTF_8);
    }

    return (T) decoded;
  }

  /**
   * Check if its a json object or not.
   *
   * Note that this code is not bullet proof, but it is copied over from as-is
   * in the spymemcached project, since its intended to be compatible with it.
   */
  private static boolean isJsonObject(final String s) {
    if (s == null || s.isEmpty()) {
      return false;
    }

    return s.startsWith("{") || s.startsWith("[")
      || "true".equals(s) || "false".equals(s)
      || "null".equals(s) || DECIMAL_PATTERN.matcher(s).matches();
  }

  protected byte[] compress(byte[] in) {
    if (in == null) {
      throw new NullPointerException("Can't compress null");
    }
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    GZIPOutputStream gz = null;
    try {
      gz = new GZIPOutputStream(bos);
      gz.write(in);
    } catch (IOException e) {
      throw new RuntimeException("IO exception compressing data", e);
    } finally {
      try {
        if (gz != null) {
          gz.close();
        }
      } catch(Exception ex) {
        // ignored.
      }
      try {
        bos.close();
      } catch(Exception ex) {
        // ignored.
      }
    }
    return bos.toByteArray();
  }

  protected byte[] decompress(byte[] in) {
    ByteArrayOutputStream bos = null;
    if(in != null) {
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      bos = new ByteArrayOutputStream();
      GZIPInputStream gis = null;
      try {
        gis = new GZIPInputStream(bis);

        byte[] buf = new byte[8192];
        int r;
        while ((r = gis.read(buf)) > 0) {
          bos.write(buf, 0, r);
        }
      } catch (IOException e) {
        bos = null;
      } finally {
        try {
          if (bos != null) {
            bos.close();
          }
        } catch(Exception ex) {
          // ignored
        }
        try {
          if (gis != null) {
            gis.close();
          }
        } catch(Exception ex) {
          // ignored
        }

        try {
          bis.close();
        } catch(Exception ex) {
          // ignored
        }
      }
    }
    return bos == null ? null : bos.toByteArray();
  }

  public static byte[] encodeNum(long l, int maxBytes) {
    byte[] rv = new byte[maxBytes];
    for (int i = 0; i < rv.length; i++) {
      int pos = rv.length - i - 1;
      rv[pos] = (byte) ((l >> (8 * i)) & 0xff);
    }
    int firstNon0 = 0;
    // Just looking for what we can reduce
    while (firstNon0 < rv.length && rv[firstNon0] == 0) {
      firstNon0++;
    }
    if (firstNon0 > 0) {
      byte[] tmp = new byte[rv.length - firstNon0];
      System.arraycopy(rv, firstNon0, tmp, 0, rv.length - firstNon0);
      rv = tmp;
    }
    return rv;
  }

  private static byte[] serialize(final Object content) {
    if (content == null) {
      throw new NullPointerException("Can't serialize null");
    }
    byte[] rv;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream os = new ObjectOutputStream(bos)) {
      os.writeObject(content);
      os.close();
      bos.close();
      rv = bos.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Non-serializable object", e);
    }
    return rv;
  }

  protected Object deserialize(byte[] in) {
    Object rv=null;
    ByteArrayInputStream bis = null;
    ObjectInputStream is = null;
    try {
      if(in != null) {
        bis=new ByteArrayInputStream(in);
        is=new ObjectInputStream(bis);
        rv=is.readObject();
        is.close();
        bis.close();
      }
    } catch (IOException e) {
      throw new CouchbaseException("Caught IOException decoding bytes of data", e);
    } catch (ClassNotFoundException e) {
      throw new CouchbaseException("Caught ClassNotFoundException decoding bytes of data", e);
    } finally {
      try {
        if (is != null) {
          is.close();
        }
      } catch(Exception ex) {
        // ignored
      }
      try {
        if (bis != null) {
          bis.close();
        }
      } catch(Exception ex) {
        // ignored
      }
    }
    return rv;
  }

  static long decodeLong(byte[] b) {
    long rv = 0;
    for (byte i : b) {
      rv = (rv << 8) | (i < 0 ? 256 + i : i);
    }
    return rv;
  }
}
