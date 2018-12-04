package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.TargetedRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.body;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.decodeStatus;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noBody;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noCas;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noDatatype;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noExtras;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noKey;
import static com.couchbase.client.core.io.netty.kv.MemcacheProtocol.noPartition;

public class BucketConfigRequest extends BaseKeyValueRequest<BucketConfigResponse> implements TargetedRequest {

  private final NetworkAddress target;

  public BucketConfigRequest(final Duration timeout, final CoreContext ctx, final String bucket,
                             final RetryStrategy retryStrategy, final NetworkAddress target) {
    super(timeout, ctx, bucket, retryStrategy, null);
    this.target = target;
  }

  @Override
  public ByteBuf encode(final ByteBufAllocator alloc, final int opaque) {
    return MemcacheProtocol.request(alloc, MemcacheProtocol.Opcode.GET_CONFIG, noDatatype(),
      noPartition(), opaque, noCas(), noExtras(), noKey(), noBody());
  }

  @Override
  public BucketConfigResponse decode(final ByteBuf response) {
    byte[] content = body(response).map(ByteBufUtil::getBytes).orElse(new byte[] {});
    return new BucketConfigResponse(decodeStatus(response), content);
  }

  @Override
  public NetworkAddress target() {
    return target;
  }
}
