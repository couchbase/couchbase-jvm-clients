package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import com.couchbase.client.core.util.Bytes;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class CarrierBucketConfigRequest extends BaseKeyValueRequest<CarrierBucketConfigResponse> implements TargetedRequest {

  private final NodeIdentifier target;

  public CarrierBucketConfigRequest(final Duration timeout, final CoreContext ctx, CollectionIdentifier collectionIdentifier,
                                    final RetryStrategy retryStrategy, final NodeIdentifier target) {
    super(timeout, ctx, retryStrategy, null, collectionIdentifier);
    this.target = target;
  }

  @Override
  public ByteBuf encode(ByteBufAllocator alloc, int opaque, ChannelContext ctx) {
    return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_CONFIG, noDatatype(),
      noPartition(), opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public CarrierBucketConfigResponse decode(final ByteBuf response, ChannelContext ctx) {
    byte[] content = body(response)
      .map(ByteBufUtil::getBytes)
      .map(bytes -> tryDecompression(bytes, datatype(response)))
      .orElse(Bytes.EMPTY_BYTE_ARRAY);
    return new CarrierBucketConfigResponse(decodeStatus(response), content);
  }

  @Override
  public NodeIdentifier target() {
    return target;
  }
}
