/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Base class for custom {@link Statement} implementations that wrap another statement.
 *
 * <p>This is intended for use with a custom {@link RetryPolicy}, {@link LoadBalancingPolicy} or
 * {@link SpeculativeExecutionPolicy}. The client code can wrap a statement to "mark" it, or add
 * information that will lead to special handling in the policy.
 *
 * <p>Example:
 *
 * <pre>{@code
 * // Define your own subclass
 * public class MyCustomStatement extends StatementWrapper {
 *     public MyCustomStatement(Statement wrapped) {
 *         super(wrapped);
 *     }
 * }
 *
 * // In your load balancing policy, add a special case for that new type
 * public class MyLoadBalancingPolicy implements LoadBalancingPolicy {
 *     public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
 *         if (statement instanceof MyCustomStatement) {
 *             // return specially crafted plan
 *         } else {
 *             // return default plan
 *         }
 *     }
 * }
 *
 * // The client wraps whenever it wants to trigger the special plan
 * Statement s = new SimpleStatement("...");
 * session.execute(s);                         // will use default plan
 * session.execute(new MyCustomStatement(s));  // will use special plan
 * }</pre>
 */
public abstract class StatementWrapper extends Statement {
  private final Statement wrapped;

  /**
   * Builds a new instance.
   *
   * @param wrapped the wrapped statement.
   */
  protected StatementWrapper(Statement wrapped) {
    this.wrapped = wrapped;
  }

  Statement getWrappedStatement() {
    // Protect against multiple levels of wrapping (even though there is no practical reason for
    // that)
    return (wrapped instanceof StatementWrapper)
        ? ((StatementWrapper) wrapped).getWrappedStatement()
        : wrapped;
  }

  @Override
  public Statement setConsistencyLevel(ConsistencyLevel consistency) {
    wrapped.setConsistencyLevel(consistency);
    return this;
  }

  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return wrapped.getConsistencyLevel();
  }

  @Override
  public Statement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
    wrapped.setSerialConsistencyLevel(serialConsistency);
    return this;
  }

  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return wrapped.getSerialConsistencyLevel();
  }

  @Override
  public Statement enableTracing() {
    wrapped.enableTracing();
    return this;
  }

  @Override
  public Statement disableTracing() {
    wrapped.disableTracing();
    return this;
  }

  @Override
  public boolean isTracing() {
    return wrapped.isTracing();
  }

  @Override
  public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    return wrapped.getRoutingKey(protocolVersion, codecRegistry);
  }

  @Override
  public String getKeyspace() {
    return wrapped.getKeyspace();
  }

  @Override
  public Statement setRetryPolicy(RetryPolicy policy) {
    wrapped.setRetryPolicy(policy);
    return this;
  }

  @Override
  public RetryPolicy getRetryPolicy() {
    return wrapped.getRetryPolicy();
  }

  @Override
  public Statement setFetchSize(int fetchSize) {
    wrapped.setFetchSize(fetchSize);
    return this;
  }

  @Override
  public int getFetchSize() {
    return wrapped.getFetchSize();
  }

  @Override
  public Statement setDefaultTimestamp(long defaultTimestamp) {
    wrapped.setDefaultTimestamp(defaultTimestamp);
    return this;
  }

  @Override
  public long getDefaultTimestamp() {
    return wrapped.getDefaultTimestamp();
  }

  @Override
  public Statement setReadTimeoutMillis(int readTimeoutMillis) {
    wrapped.setReadTimeoutMillis(readTimeoutMillis);
    return this;
  }

  @Override
  public int getReadTimeoutMillis() {
    return wrapped.getReadTimeoutMillis();
  }

  @Override
  public Statement setPagingState(PagingState pagingState, CodecRegistry codecRegistry) {
    wrapped.setPagingState(pagingState, codecRegistry);
    return this;
  }

  @Override
  public Statement setPagingState(PagingState pagingState) {
    wrapped.setPagingState(pagingState);
    return this;
  }

  @Override
  public Statement setPagingStateUnsafe(byte[] pagingState) {
    wrapped.setPagingStateUnsafe(pagingState);
    return this;
  }

  @Override
  public ByteBuffer getPagingState() {
    return wrapped.getPagingState();
  }

  @Override
  public Statement setIdempotent(boolean idempotent) {
    wrapped.setIdempotent(idempotent);
    return this;
  }

  @Override
  public Boolean isIdempotent() {
    return wrapped.isIdempotent();
  }

  @Override
  public boolean isIdempotentWithDefault(QueryOptions queryOptions) {
    return wrapped.isIdempotentWithDefault(queryOptions);
  }

  @Override
  public Map<String, ByteBuffer> getOutgoingPayload() {
    return wrapped.getOutgoingPayload();
  }

  @Override
  public Statement setOutgoingPayload(Map<String, ByteBuffer> payload) {
    wrapped.setOutgoingPayload(payload);
    return this;
  }

  @Override
  public int requestSizeInBytes(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    return wrapped.requestSizeInBytes(protocolVersion, codecRegistry);
  }

  @Override
  public Host getHost() {
    return wrapped.getHost();
  }

  @Override
  public Statement setHost(Host host) {
    wrapped.setHost(host);
    return this;
  }
}
