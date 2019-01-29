package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.kv.ChannelContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.*;

public class CarrierBucketConfigRequest extends BaseKeyValueRequest<CarrierBucketConfigResponse> implements TargetedRequest {

  private final NetworkAddress target;

  public CarrierBucketConfigRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                                    final RetryStrategy retryStrategy, final NetworkAddress target) {
    super(timeout, ctx, bucket, retryStrategy, null, null);
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
      .orElse(new byte[] {});
    return new CarrierBucketConfigResponse(decodeStatus(response), content);
  }

  @Override
  public NetworkAddress target() {
    return target;
  }
}
