/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.io.netty;

import com.couchbase.client.core.deps.io.netty.channel.ChannelHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslHandshakeCompletionEvent;
import com.couchbase.client.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Base64;

import static com.couchbase.client.core.util.CbThrowables.getStackTraceAsString;

@ChannelHandler.Sharable
public class SslSessionLoggingHandler extends ChannelInboundHandlerAdapter {
  private static final Logger log = LoggerFactory.getLogger(SslSessionLoggingHandler.class);

  public static final SslSessionLoggingHandler INSTANCE = new SslSessionLoggingHandler();

  private SslSessionLoggingHandler() {
  }

  public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
    if (event instanceof SslHandshakeCompletionEvent) {
      try {
        SslHandler handler = ctx.pipeline().get(SslHandler.class);
        log(handler.engine().getSession());
      } catch (SSLPeerUnverifiedException ignore) {
        // Certificate verification failed. Can't access the server certificates
        // from here, so there's nothing to do.
      } finally {
        ctx.pipeline().remove(this);
        ctx.fireUserEventTriggered(event);
      }
    }
  }

  private static void log(SSLSession session) throws SSLPeerUnverifiedException {
    if (!log.isDebugEnabled()) {
      return;
    }

    StringBuilder pemChain = new StringBuilder();
    try {
      for (Certificate cert : session.getPeerCertificates()) {
        String base64Encoded = Base64.getMimeEncoder().encodeToString(cert.getEncoded());
        pemChain.append("-----BEGIN CERTIFICATE-----\n");
        pemChain.append(base64Encoded).append("\n");
        pemChain.append("-----END CERTIFICATE-----\n");
      }
    } catch (CertificateEncodingException e) {
      pemChain.setLength(0);
      pemChain.append("Can't display encoded certificate chain; ");
      pemChain.append(getStackTraceAsString(e));
    }

    log.debug(
      "TLS handshake complete! remote = {} ; cipher suite = {} ; certificate chain = \n{}",
      new HostAndPort(session.getPeerHost(), session.getPeerPort()),
      session.getCipherSuite(),
      pemChain
    );
  }
}
