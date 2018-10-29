package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

public class NoopRequest extends BaseKeyValueRequest<NoopResponse> {

  public NoopRequest(final Duration timeout, final RequestContext ctx) {
    super(timeout, ctx);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque) {
    return MemcacheProtocol.request(
      alloc,
      MemcacheProtocol.Opcode.NOOP,
      noDatatype(),
      noPartition(),
      opaque,
      noCas(),
      noExtras(),
      noKey(),
      noBody()
    );
  }

  @Override
  public NoopResponse decode(ByteBuf response) {
    return new NoopResponse(MemcacheProtocol.decodeStatus(response));
  }
}
