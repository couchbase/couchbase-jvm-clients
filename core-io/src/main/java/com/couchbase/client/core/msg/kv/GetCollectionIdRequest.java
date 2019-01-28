package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.extras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.request;

public class GetCollectionIdRequest extends BaseKeyValueRequest<GetCollectionIdResponse> {

  private final String scopeName;
  private final String collectionName;

  public GetCollectionIdRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                                final RetryStrategy retryStrategy,
                                final String scopeName, final String collectionName) {
    super(timeout, ctx, bucket, retryStrategy, null, null);
    this.scopeName = scopeName;
    this.collectionName = collectionName;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    ByteBuf key = Unpooled.copiedBuffer(scopeName + "." + collectionName, CharsetUtil.UTF_8);
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
