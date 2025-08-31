/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import org.jctools.queues.MessagePassingQueue;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * An object pool that is "pinned" to a specific thread for consumption,
 * but can accept objects back from any thread.
 * <p>
 * This recycler is designed as a more efficient replacement for the standard {@link Recycler}
 * in scenarios where an object pool is owned by a single thread (e.g., an event loop).
 * It avoids the overhead of {@link FastThreadLocal} and provides
 * a more direct and efficient two-queue (local/external) system.
 *
 * @param <T> The type of the object to recycle.
 */
public final class PinnedRecycler<T> {

    // A mask to control drain frequency. Drains every 256 calls to get().
    private static final int DRAIN_MASK = 255;

    private final int maxCapacity;
    private final Thread ownerThread;
    private final ArrayDeque<T> localQueue;
    private final Queue<T> externalQueue;

    private int drainCounter;

    /**
     * A handle that allows an object to recycle itself back to its owning recycler.
     * @param <T> The type of the object to recycle.
     */
    public interface Handle<T> {
        void recycle(T object);
    }

    /**
     * Creates a new PinnedRecycler.
     *
     * @param maxCapacity The maximum number of objects to be retained in the pool.
     */
    public PinnedRecycler(int maxCapacity) {
        this.maxCapacity = ObjectUtil.checkPositive(maxCapacity, "maxCapacity");
        this.ownerThread = Thread.currentThread();
        this.localQueue = new ArrayDeque<>(maxCapacity);
        this.externalQueue = PlatformDependent.newFixedMpscQueue(maxCapacity);
    }

    /**
     * Retrieves an object from the recycler.
     * <p>
     * This method is optimized for the owner thread and should not be called
     * from other threads.
     *
     * @return A recycled or new object.
     */
    public T get() {
        assert Thread.currentThread() == ownerThread;

        if ((drainCounter++ & DRAIN_MASK) == 0) {
            drainExternalToLocal();
        }

        T obj = localQueue.pollLast();
        if (obj == null) {
            obj = externalQueue.poll();
        }
        return obj;
    }

    public void recycle(T object) {
        if (Thread.currentThread() == ownerThread) {
            if (localQueue.size() < maxCapacity) {
                localQueue.addLast(object);
            }
        } else {
            externalQueue.offer(object);
        }
    }

    /**
     * Drains objects from the thread-safe external queue to the fast local queue.
     */
    private void drainExternalToLocal() {
        @SuppressWarnings("unchecked")
        final MessagePassingQueue<T> mpscQueue = (MessagePassingQueue<T>) externalQueue;

        final int limit = maxCapacity - localQueue.size();
        if (limit > 0) {
            mpscQueue.drain(localQueue::addLast, limit);
        }
    }
}
