/*
 * Copyright © 2019 Apple Inc. and the ServiceTalk project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.servicetalk.http.netty;

import io.servicetalk.transport.netty.internal.ChannelInitializer;

import io.netty.channel.Channel;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2MultiplexHandler;

import java.util.function.BiPredicate;

import static io.netty.handler.codec.http2.Http2FrameCodecBuilder.forServer;
import static io.netty.handler.logging.LogLevel.TRACE;

final class H2ServerParentChannelInitializer implements ChannelInitializer {
    private final H2ProtocolConfig config;
    private final io.netty.channel.ChannelInitializer<Channel> streamChannelInitializer;

    H2ServerParentChannelInitializer(final H2ProtocolConfig config,
                                     final io.netty.channel.ChannelInitializer<Channel> streamChannelInitializer) {
        this.config = config;
        this.streamChannelInitializer = streamChannelInitializer;
    }

    @Override
    public void init(final Channel channel) {
        final Http2FrameCodecBuilder multiplexCodecBuilder = forServer()
                // We don't want to rely upon Netty to manage the graceful close timeout, because we expect
                // the user to apply their own timeout at the call site.
                .gracefulShutdownTimeoutMillis(-1);

        final BiPredicate<CharSequence, CharSequence> headersSensitivityDetector =
                config.headersSensitivityDetector();
        multiplexCodecBuilder.headerSensitivityDetector(headersSensitivityDetector::test);

        final String frameLoggerName = config.frameLoggerName();
        if (frameLoggerName != null) {
            multiplexCodecBuilder.frameLogger(new Http2FrameLogger(TRACE, frameLoggerName));
        }

        // TODO(scott): more configuration. header validation, settings stream, etc...

        channel.pipeline().addLast(multiplexCodecBuilder.build(), new Http2MultiplexHandler(streamChannelInitializer));
    }
}
