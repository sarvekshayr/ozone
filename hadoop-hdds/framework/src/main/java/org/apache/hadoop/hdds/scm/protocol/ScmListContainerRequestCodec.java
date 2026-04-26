/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.protocol;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto.Builder;

/**
 * Codec between {@link SCMListContainerRequestProto} and {@link ListContainerQuery} for list containers.
 */
public final class ScmListContainerRequestCodec {

  private ScmListContainerRequestCodec() {
  }

  /**
   * Immutable view of list container request fields after {@link #fromProto}.
   */
  public static final class ListContainerQuery {
    private final long startContainerID;
    private final int count;
    @Nullable
    private final HddsProtos.LifeCycleState state;
    @Nullable
    private final HddsProtos.ReplicationFactor factor;
    @Nullable
    private final HddsProtos.ReplicationType replicationType;
    @Nullable
    private final ReplicationConfig replicationConfig;
    @Nullable
    private final Boolean suppressed;

    private ListContainerQuery(long startContainerID, int count,
        @Nullable HddsProtos.LifeCycleState state,
        @Nullable HddsProtos.ReplicationFactor factor,
        @Nullable HddsProtos.ReplicationType replicationType,
        @Nullable ReplicationConfig replicationConfig,
        @Nullable Boolean suppressed) {
      this.startContainerID = startContainerID;
      this.count = count;
      this.state = state;
      this.factor = factor;
      this.replicationType = replicationType;
      this.replicationConfig = replicationConfig;
      this.suppressed = suppressed;
    }

    public long getStartContainerID() {
      return startContainerID;
    }

    public int getCount() {
      return count;
    }

    @Nullable
    public HddsProtos.LifeCycleState getState() {
      return state;
    }

    @Nullable
    public HddsProtos.ReplicationFactor getFactor() {
      return factor;
    }

    @Nullable
    public HddsProtos.ReplicationType getReplicationType() {
      return replicationType;
    }

    @Nullable
    public ReplicationConfig getReplicationConfig() {
      return replicationConfig;
    }

    @Nullable
    public Boolean getSuppressed() {
      return suppressed;
    }
  }

  /**
   * Builds {@link SCMListContainerRequestProto} from Java fields.
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  public static SCMListContainerRequestProto toProto(
      long startContainerID,
      int count,
      @Nullable HddsProtos.LifeCycleState state,
      @Nullable HddsProtos.ReplicationFactor factor,
      @Nullable HddsProtos.ReplicationType replicationType,
      @Nullable ReplicationConfig repConfig,
      @Nullable Boolean suppressed,
      @Nullable String traceId) {
    Builder builder = SCMListContainerRequestProto.newBuilder()
        .setCount(count)
        .setStartContainerID(startContainerID);
    if (traceId != null) {
      builder.setTraceID(traceId);
    }
    if (suppressed != null) {
      builder.setSuppressed(suppressed);
    }
    if (state != null) {
      builder.setState(state);
    }
    if (repConfig != null) {
      if (repConfig.getReplicationType() == EC) {
        builder.setType(EC);
        builder.setEcReplicationConfig(((ECReplicationConfig) repConfig).toProto());
      } else {
        builder.setType(repConfig.getReplicationType());
        builder.setFactor(((ReplicatedReplicationConfig) repConfig)
            .getReplicationFactor());
      }
    } else if (replicationType != null) {
      builder.setType(replicationType);
    } else if (factor != null) {
      builder.setFactor(factor);
    }
    return builder.build();
  }

  /**
   * Decodes {@link SCMListContainerRequestProto} into {@link ListContainerQuery}.
   */
  public static ListContainerQuery fromProto(SCMListContainerRequestProto request) {
    long startContainerID = 0L;
    if (request.hasStartContainerID()) {
      startContainerID = request.getStartContainerID();
    }
    int count = request.getCount();
    HddsProtos.LifeCycleState state = null;
    HddsProtos.ReplicationFactor factor = null;
    HddsProtos.ReplicationType replicationType = null;
    ReplicationConfig repConfig = null;
    if (request.hasState()) {
      state = request.getState();
    }
    if (request.hasType()) {
      replicationType = request.getType();
    }
    if (replicationType != null) {
      if (replicationType == HddsProtos.ReplicationType.EC) {
        if (request.hasEcReplicationConfig()) {
          repConfig = new ECReplicationConfig(request.getEcReplicationConfig());
        }
      } else {
        if (request.hasFactor()) {
          repConfig = ReplicationConfig
              .fromProtoTypeAndFactor(request.getType(), request.getFactor());
        }
      }
    } else if (request.hasFactor()) {
      factor = request.getFactor();
    }
    Boolean suppressed = request.hasSuppressed() ? request.getSuppressed() : null;
    return new ListContainerQuery(startContainerID, count, state, factor,
        replicationType, repConfig, suppressed);
  }
}
