package com.couchbase.client.core.io;

import com.couchbase.client.core.CoreContext;

import java.net.SocketAddress;
import java.util.Map;

public class IoContext extends CoreContext {

    private final SocketAddress localSocket;

    private final SocketAddress remoteSocket;

    public IoContext(final CoreContext ctx, final SocketAddress localSocket,
                     final SocketAddress remoteSocket) {
        super(ctx.id(), ctx.env());
        this.localSocket = localSocket;
        this.remoteSocket = remoteSocket;
    }

    @Override
    protected void injectExportableParams(final Map<String, Object> input) {
        super.injectExportableParams(input);
        input.put("local", localSocket());
        input.put("remote", remoteSocket());
    }

    public SocketAddress localSocket() {
        return localSocket;
    }

    public SocketAddress remoteSocket() {
        return remoteSocket;
    }
}