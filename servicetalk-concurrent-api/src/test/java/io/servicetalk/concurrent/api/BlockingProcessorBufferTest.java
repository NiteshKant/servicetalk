/*
 * Copyright © 2020 Apple Inc. and the ServiceTalk project authors
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
package io.servicetalk.concurrent.api;

import io.servicetalk.concurrent.internal.ServiceTalkTestTimeout;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeoutException;

import static io.servicetalk.concurrent.internal.DeliberateException.DELIBERATE_EXCEPTION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class BlockingProcessorBufferTest {
    @Rule
    public final Timeout timeout = new ServiceTalkTestTimeout();

    private final DefaultBlockingProcessorBuffer<Integer> buffer;
    @SuppressWarnings("unchecked")
    private final BufferConsumer<Integer> consumer = mock(BufferConsumer.class);

    public BlockingProcessorBufferTest() {
        buffer = new DefaultBlockingProcessorBuffer<>(1);
    }

    @Test
    public void consumeItem() throws Exception {
        buffer.add(1);
        assertThat("Item not consumed.", buffer.consume(consumer), is(true));
        verify(consumer).consumeItem(1);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void consumeEmpty() {
        assertThrows("Unexpected consume when empty.", TimeoutException.class,
                () -> buffer.consume(consumer, 1, MILLISECONDS));
        verifyZeroInteractions(consumer);
    }

    @Test
    public void consumeTerminal() throws Exception {
        buffer.terminate();
        assertThat("Item not consumed.", buffer.consume(consumer), is(true));
        verify(consumer).consumeTerminal();
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void consumeTerminalError() throws Exception {
        buffer.terminate(DELIBERATE_EXCEPTION);
        assertThat("Item not consumed.", buffer.consume(consumer), is(true));
        verify(consumer).consumeTerminal(DELIBERATE_EXCEPTION);
        verifyNoMoreInteractions(consumer);
    }
}
