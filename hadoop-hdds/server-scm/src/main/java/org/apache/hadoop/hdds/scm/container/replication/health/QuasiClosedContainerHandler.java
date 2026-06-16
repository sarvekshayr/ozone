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

package org.apache.hadoop.hdds.scm.container.replication.health;

import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling containers that are in QUASI_CLOSED state. This will
 * send commands to Datanodes to force close these containers if they satisfy
 * the requirements to be force closed. Only meant for RATIS containers.
 *
 * Note - this handler always returns false so further handlers can can for
 * under and over replication etc.
 */
public class QuasiClosedContainerHandler extends AbstractCheck {
  private static final Logger LOG =
      LoggerFactory.getLogger(QuasiClosedContainerHandler.class);

  private final ReplicationManager replicationManager;

  public QuasiClosedContainerHandler(ReplicationManager replicationManager) {
    this.replicationManager = replicationManager;
  }

  /**
   * If possible, force closes the Ratis container in QUASI_CLOSED state.
   * Replicas with the highest Sequence ID are selected to be closed.
   * @param request ContainerCheckRequest object representing the container
   * @return true if close commands were sent, otherwise false
   */
  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    if (containerInfo.getReplicationType() !=
        HddsProtos.ReplicationType.RATIS) {
      return false;
    }

    if (containerInfo.getState() != HddsProtos.LifeCycleState.QUASI_CLOSED) {
      return false;
    }
    LOG.debug("Checking container {} in QuasiClosedContainerHandler",
        containerInfo);

    Set<ContainerReplica> replicas = request.getContainerReplicas();
    DatanodeID pipelineLeaderId = replicationManager.getPipelineLeaderId(containerInfo);
    if (canForceCloseContainer(containerInfo, replicas, pipelineLeaderId)) {
      if (!request.isReadOnly()) {
        forceCloseContainer(containerInfo, replicas);
      }
    } else {
      LOG.debug("Container {} cannot be force closed and is stuck in " +
              "QUASI_CLOSED", containerInfo);
      request.getReport().incrementAndSample(ContainerHealthState.QUASI_CLOSED_STUCK, containerInfo);
    }
    // Always return false, even if commands were sent. That way, under and
    // over replication handlers can to check for other issues in the container.
    return false;
  }

  /**
   * Returns true if the container is stuck in QUASI_CLOSED state, otherwise false.
   * @param container The container to check
   * @param replicas Set of ContainerReplicas
   * @param pipelineLeaderId last known pipeline leader, if available
   * @return true if the container is stuck in QUASI_CLOSED state, otherwise false
   */
  public static boolean isQuasiClosedStuck(final ContainerInfo container,
      final Set<ContainerReplica> replicas, @Nullable DatanodeID pipelineLeaderId) {
    return !canForceCloseContainer(container, replicas, pipelineLeaderId);
  }

  /**
   * Returns true if more than 50% of the container replicas with unique
   * originNodeId are in QUASI_CLOSED state.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @param pipelineLeaderId last known pipeline leader, if available
   * @return true if we can force close the container, false otherwise
   */
  private static boolean canForceCloseContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas, @Nullable DatanodeID pipelineLeaderId) {
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();

    long maxQCSeq = -1;
    long maxUnhealthySeq = -1;
    long quasiClosedOrUnhealthyCount = 0;
    final Set<DatanodeID> quasiClosedOrUnhealthyOrigins = new HashSet<>();
    for (ContainerReplica r : replicas) {
      if (r.getState() == State.QUASI_CLOSED) {
        maxQCSeq = Math.max(maxQCSeq, r.getSequenceId());
        quasiClosedOrUnhealthyCount++;
        quasiClosedOrUnhealthyOrigins.add(r.getOriginDatanodeId());
      } else if (r.getState() == State.UNHEALTHY) {
        maxUnhealthySeq = Math.max(maxUnhealthySeq, r.getSequenceId());
        quasiClosedOrUnhealthyCount++;
        quasiClosedOrUnhealthyOrigins.add(r.getOriginDatanodeId());
      }
    }

    if (maxQCSeq <= -1 || maxQCSeq < maxUnhealthySeq) {
      return false;
    }
    
    // We can only force close the container if we have seen all the replicas from unique origins.
    // Due to unexpected behavior when writing to ratis containers, it is possible for blocks to be committed
    // on the ratis leader, but not on the followers. A failure on the leader can result in two replicas
    // without the latest transactions, which are then force closed. This can result in data loss.
    // Note that if the 3rd replica is permanently lost, the container will be stuck in QUASI_CLOSED state forever.
    // It is possible to CLOSE a container that has one QC and the remaining UNHEALTHY, provided the QC is one of the
    // replicas with the highest sequence ID. If an UNHEALTHY replica has a higher sequence ID, the container will
    // remain in QUASI_CLOSED state.
    final long uniqueQuasiClosedOrUnhealthyReplicaCount = quasiClosedOrUnhealthyOrigins.size();
    if (uniqueQuasiClosedOrUnhealthyReplicaCount >= replicationFactor) {
      return true;
    }

    return canForceCloseWithPipelineLeader(replicas, pipelineLeaderId, replicationFactor, 
        uniqueQuasiClosedOrUnhealthyReplicaCount, quasiClosedOrUnhealthyCount, maxQCSeq);
  }

  /**
   * Allow force close with fewer unique origins when the last pipeline leader's
   * QUASI_CLOSED replica has the highest BCSID.
   */
  private static boolean canForceCloseWithPipelineLeader(Set<ContainerReplica> replicas,
      @Nullable DatanodeID pipelineLeaderId, int replicationFactor, long uniqueOriginCount,
      long quasiClosedOrUnhealthyCount, long maxQCSeq) {
    if (pipelineLeaderId == null) {
      return false;
    }
    if (uniqueOriginCount < 2 || quasiClosedOrUnhealthyCount < replicationFactor) {
      return false;
    }
    return leaderHasQuasiClosedReplicaAtMaxBcsId(replicas, pipelineLeaderId, maxQCSeq);
  }

  private static boolean leaderHasQuasiClosedReplicaAtMaxBcsId(Set<ContainerReplica> replicas,
      DatanodeID pipelineLeaderId, long maxQCSeq) {
    for (ContainerReplica replica : replicas) {
      if (replica.getState() != State.QUASI_CLOSED) {
        continue;
      }
      if (!pipelineLeaderId.equals(replica.getOriginDatanodeId())) {
        continue;
      }
      if (replica.getSequenceId() == null) {
        continue;
      }
      if (replica.getSequenceId() == maxQCSeq) {
        return true;
      }
    }
    return false;
  }

  /**
   * Force close the container replica(s) with the highest Sequence ID.
   *
   * <p>
   *   Note: We should force close the container only if >50% (quorum)
   *   of replicas with unique originNodeId are in QUASI_CLOSED state.
   * </p>
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void forceCloseContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    final List<ContainerReplica> quasiClosedReplicas = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .collect(Collectors.toList());

    final Long sequenceId = quasiClosedReplicas.stream()
        .map(ContainerReplica::getSequenceId)
        .max(Long::compare)
        .orElse(-1L);

    LOG.info("Force closing container {} with BCSID {}, which is in " +
            "QUASI_CLOSED state.", container.containerID(), sequenceId);

    quasiClosedReplicas.stream()
        .filter(r -> sequenceId != -1L)
        .filter(replica -> replica.getSequenceId().equals(sequenceId))
        .forEach(replica -> replicationManager.sendCloseContainerReplicaCommand(
            container, replica.getDatanodeDetails(), true));
  }
}
