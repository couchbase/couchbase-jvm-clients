package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.subdoc.SubDocumentException;
import io.netty.util.CharsetUtil;

import java.util.Optional;

// TODO rename SubDocumentField as part of public interface now
public class SubdocField {
  private final SubDocumentOpResponseStatus status;
  private final Optional<SubDocumentException> error;
  private final byte[] value;
  private final String path;
  private final SubdocCommandType type;

  public SubdocField(SubDocumentOpResponseStatus status,
                     Optional<SubDocumentException> error,
                     byte[] value,
                     String path,
                     SubdocCommandType type) {
    this.status = status;
    this.error = error;
    this.value = value;
    this.path = path;
    this.type = type;
  }

  public SubDocumentOpResponseStatus status() {
    return status;
  }

  public Optional<SubDocumentException> error() {
    return error;
  }

  public byte[] value() {
    return value;
  }

  public String path() {
    return path;
  }

  public SubdocCommandType type() {
    return type;
  }

  @Override
  public String toString() {
    return "SubdocField{" +
      "status=" + status +
      ", value=" + new String(value, CharsetUtil.UTF_8) +
      ", path='" + path + '\'' +
      '}';
  }
}
