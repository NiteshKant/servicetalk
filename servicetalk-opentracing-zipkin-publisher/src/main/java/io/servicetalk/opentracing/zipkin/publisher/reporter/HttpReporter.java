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
package io.servicetalk.opentracing.zipkin.publisher.reporter;

import io.servicetalk.buffer.api.BufferAllocator;
import io.servicetalk.concurrent.Cancellable;
import io.servicetalk.concurrent.PublisherSource;
import io.servicetalk.concurrent.api.AsyncCloseable;
import io.servicetalk.concurrent.api.Completable;
import io.servicetalk.concurrent.api.TerminalSignalConsumer;
import io.servicetalk.http.api.HttpClient;
import io.servicetalk.http.api.HttpRequest;
import io.servicetalk.http.api.SingleAddressHttpClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Component;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Reporter;

import static io.servicetalk.concurrent.api.AsyncCloseables.newCompositeCloseable;
import static io.servicetalk.concurrent.api.AsyncCloseables.toAsyncCloseable;
import static io.servicetalk.concurrent.api.Completable.completed;
import static io.servicetalk.concurrent.api.Processors.newPublisherProcessor;
import static io.servicetalk.concurrent.api.PublisherProcessorBuffers.fixedSizeDropOldest;
import static io.servicetalk.concurrent.api.SourceAdapters.fromSource;
import static io.servicetalk.concurrent.internal.FutureUtils.awaitTermination;
import static io.servicetalk.http.api.HttpHeaderNames.CONTENT_TYPE;
import static io.servicetalk.http.api.HttpHeaderValues.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

/**
 * A {@link Span} {@link Reporter} that will publish to an HTTP endpoint with a configurable encoding {@link Codec}.
 */
public final class HttpReporter extends Component implements Reporter<Span>, AsyncCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpReporter.class);
    private final HttpClient client;
    private final String path;
    private final CharSequence contentType;
    private final SpanBytesEncoder spanEncoder;
    private final BufferAllocator allocator;
    private final PublisherSource.Processor<Span, Span> buffer;
    private final AsyncCloseable closeable;
    private final Cancellable requestSendCancellable;

    private HttpReporter(final Builder builder) {
        client = builder.clientBuilder.build();
        spanEncoder = builder.codec.spanBytesEncoder();
        switch (builder.codec) {
            case JSON_V1:
                path = "/api/v1/spans";
                contentType = APPLICATION_JSON;
                break;
            case JSON_V2:
                path = "/api/v2/spans";
                contentType = APPLICATION_JSON;
                break;
            case THRIFT:
                path = "/api/v2/spans";
                contentType = "application/x-thrift";
                break;
            case PROTO3:
                path = "/api/v2/spans";
                contentType = "application/protobuf";
                break;
            default:
                throw new IllegalArgumentException("Unknown codec: " + builder.codec);
        }
        allocator = client.executionContext().bufferAllocator();
        //TODO: Configure strategy
        buffer = newPublisherProcessor(fixedSizeDropOldest(256));
        //TODO: Use buffer
        requestSendCancellable = fromSource(buffer)
                .flatMapCompletable(span -> client.request(newRequest(spanEncoder.encode(span)))
                        .ignoreElement()
                        .onErrorResume(cause -> {
                            LOGGER.error("Failed to send a span, ignoring.", cause);
                            return completed();
                        }))
                .beforeFinally(new TerminalSignalConsumer() {
                    @Override
                    public void onComplete() {
                        LOGGER.error("Span buffer terminated, no more spans will be sent.");
                    }

                    @Override
                    public void onError(final Throwable throwable) {
                        LOGGER.error("Span buffer terminated with an error, no more spans will be sent.", throwable);
                    }

                    @Override
                    public void cancel() {
                        // noop (closed)
                    }
                })
                .subscribe();
        closeable = newCompositeCloseable().appendAll(toAsyncCloseable(__ -> {
            try {
                requestSendCancellable.cancel();
            } catch (Throwable t) {
                LOGGER.error("Failed to cancel request sending. Ignoring.", t);
            }
            try {
                buffer.onComplete();
            } catch (Throwable t) {
                LOGGER.error("Failed to dispose request buffer. Ignoring.", t);
            }
            return completed();
        })).append(client);
    }

    private HttpRequest newRequest(final byte[] payload) {
        return client.post(path).addHeader(CONTENT_TYPE, contentType).payloadBody(allocator.wrap(payload));
    }

    /**
     * A builder to create a new {@link HttpReporter}.
     */
    public static final class Builder {
        private Codec codec = Codec.JSON_V2;
        private final SingleAddressHttpClientBuilder<?, ?> clientBuilder;

        /**
         * Create a new {@link Builder} using the passed {@link SingleAddressHttpClientBuilder}.
         *
         * @param clientBuilder the collector SocketAddress
         */
        public Builder(final SingleAddressHttpClientBuilder<?, ?> clientBuilder) {
            this.clientBuilder = clientBuilder;
        }

        /**
         * Sets the {@link Codec} to encode the Spans with.
         *
         * @param codec the codec to use for this span.
         * @return {@code this}
         */
        public Builder codec(Codec codec) {
            this.codec = requireNonNull(codec);
            return this;
        }

        /**
         * Builds a new {@link HttpReporter} instance with this builder's options.
         * <p>
         * This method may block while the underlying UDP channel is being bound.
         *
         * @return a new {@link HttpReporter}
         */
        public HttpReporter build() {
            return new HttpReporter(this);
        }
    }

    @Override
    public void report(final Span span) {
        buffer.onNext(span);
    }

    @Override
    public void close() {
        awaitTermination(closeable.closeAsync().toFuture());
    }

    @Override
    public Completable closeAsync() {
        return closeable.closeAsync();
    }

    @Override
    public Completable closeAsyncGracefully() {
        return closeable.closeAsyncGracefully();
    }
}
