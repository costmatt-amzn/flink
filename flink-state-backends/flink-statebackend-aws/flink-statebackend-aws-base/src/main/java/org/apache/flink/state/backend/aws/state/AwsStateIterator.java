/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.backend.aws.state;

import io.reactivex.rxjava3.core.Flowable;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.util.function.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for AWS state backends that implements both Iterator and SdkIterable interfaces.
 *
 * <p>This class provides a unified way to iterate over state entries stored in AWS services
 * like DynamoDB or S3. It handles pagination of results from AWS SDK responses and converts
 * them to Flink state entries.
 *
 * <p>The iterator can be created from either:
 * <ul>
 *   <li>A synchronous {@link SdkIterable} from AWS SDK v2 sync clients</li>
 *   <li>An asynchronous {@link SdkPublisher} from AWS SDK v2 async clients</li>
 * </ul>
 *
 * <p>This class is used during state access operations like:
 * <ul>
 *   <li>Checkpoint creation to iterate over all state entries</li>
 *   <li>State queries that need to scan multiple entries</li>
 *   <li>Operations that need to process all keys for a specific namespace</li>
 * </ul>
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the state value.
 * @param <R> The type of the AWS response (e.g., QueryResponse, ListObjectsResponse).
 */
public class AwsStateIterator<K, N, SV, R>
        implements Iterator<StateEntry<K, N, SV>>,
        SdkIterable<StateEntry<K, N, SV>> {

    private static final Logger LOG = LoggerFactory.getLogger(AwsStateIterator.class);

    private final Iterator<R> responseIt;
    private final SerializableFunction<R, Iterator<? extends StateEntry<K, N, SV>>> responseMapper;

    private Iterator<? extends StateEntry<K, N, SV>> currentPageIt = Collections.emptyIterator();

    /**
     * Creates a new AwsStateIterator from a sync client SdkIterable.
     *
     * <p>This constructor is used with synchronous AWS SDK clients that return
     * {@link SdkIterable} objects for paginated results.
     *
     * @param paginator The AWS SDK paginator that provides pages of results
     * @param responseMapper A function that maps each AWS response to an iterator of state entries
     */
    public AwsStateIterator(
            final SdkIterable<R> paginator,
            final SerializableFunction<R, Iterator<? extends StateEntry<K, N, SV>>> responseMapper) {
        LOG.debug("Creating AwsStateIterator from SdkIterable");
        this.responseIt = paginator.iterator();
        this.responseMapper = responseMapper;
        LOG.trace("AwsStateIterator created successfully from SdkIterable");
    }

    /**
     * Creates a new AwsStateIterator from an Async client SdkPublisher.
     *
     * <p>This constructor is used with asynchronous AWS SDK clients that return
     * {@link SdkPublisher} objects for paginated results. The publisher is converted
     * to a blocking iterable using RxJava's Flowable.
     *
     * @param paginator The AWS SDK publisher that provides pages of results
     * @param responseMapper A function that maps each AWS response to an iterator of state entries
     */
    public AwsStateIterator(
            final SdkPublisher<R> paginator,
            final SerializableFunction<R, Iterator<? extends StateEntry<K, N, SV>>> responseMapper) {
        LOG.debug("Creating AwsStateIterator from SdkPublisher");
        this.responseIt = Flowable.fromPublisher(paginator).blockingIterable().iterator();
        this.responseMapper = responseMapper;
        LOG.trace("AwsStateIterator created successfully from SdkPublisher");
    }

    /**
     * Returns {@code true} if there are more state entries available.
     *
     * @return {@code true} if there are more state entries available, {@code false} otherwise
     */
    @Override
    public boolean hasNext() {
        boolean hasNext = responseIt.hasNext() || currentPageIt.hasNext();
        LOG.trace("AwsStateIterator.hasNext() = {}", hasNext);
        return hasNext;
    }

    /**
     * Returns the next state entry.
     *
     * <p>This method advances to the next page of results if necessary.
     *
     * @return The next state entry as a map of attribute names to values
     * @throws NoSuchElementException if there are no more state entries available
     */
    @Override
    public StateEntry<K, N, SV> next() {
        if (!currentPageIt.hasNext() && responseIt.hasNext()) {
            currentPageIt = responseMapper.apply(responseIt.next());
        }

        if (currentPageIt.hasNext()) {
            return currentPageIt.next();
        }

        throw new NoSuchElementException();
    }

    @Nonnull
    @Override
    public Iterator<StateEntry<K, N, SV>> iterator() {
        return this;
    }
}
