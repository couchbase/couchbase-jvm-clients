package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GetCollectionIdRequest extends BaseKeyValueRequest<GetCollectionIdResponse> {

  public GetCollectionIdRequest(final Duration timeout, final CoreContext ctx,
                                final RetryStrategy retryStrategy,
                                CollectionIdentifier collectionIdentifier) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.copiedBuffer(collectionIdentifier().scope() + "." + collectionIdentifier().collection(), UTF_8);
    ByteBuf request = request(alloc, MemcacheProtocol.Opcode.COLLECTIONS_GET_CID, noDatatype(),
      noPartition(), opaque, noCas(), noExtras(), key, noBody());
    key.release();
    return request;
  }

  @Override
  public GetCollectionIdResponse decode(ByteBuf response, ChannelContext ctx) {
    ResponseStatus status = MemcacheProtocol.decodeStatus(response);
    Optional<Long> cid = Optional.empty();
    if (status.success()) {
      cid = Optional.of(extras(response).get().getUnsignedInt(8));
    }
    return new GetCollectionIdResponse(status, cid);
  }
}
