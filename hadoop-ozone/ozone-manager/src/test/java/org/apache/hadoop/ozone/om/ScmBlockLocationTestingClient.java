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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result;
import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result.success;
import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result.unknownFailure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.InnerNodeImpl;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a testing client that allows us to intercept calls from OzoneManager
 * to SCM.
 * <p>
 * TODO: OzoneManager#getScmBlockClient -- so that we can load this class up via
 * config setting into OzoneManager. Right now, we just pass this to
 * KeyDeletingService only.
 * <p>
 * TODO: Move this class to a generic test utils so we can use this class in
 * other Ozone Manager tests.
 */
public class ScmBlockLocationTestingClient implements ScmBlockLocationProtocol {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmBlockLocationTestingClient.class);
  private final String clusterID;
  private final String scmId;

  // 0 means no calls will fail, +1 means all calls will fail, +2 means every
  // second call will fail, +3 means every third and so on.
  private final int failCallsFrequency;
  private int currentCall = 0;

  // The number of blocks deleted by this client
  private int numBlocksDeleted = 0;

  /**
   * If ClusterID or SCMID is blank a per instance ID is generated.
   *
   * @param clusterID - String or blank.
   * @param scmId - String or Blank.
   * @param failCallsFrequency - Set to 0 for no failures, 1 for always to fail,
   * a positive number for that frequency of failure.
   */
  public ScmBlockLocationTestingClient(String clusterID, String scmId,
      int failCallsFrequency) {
    this.clusterID = StringUtils.isNotBlank(clusterID) ? clusterID :
        UUID.randomUUID().toString();
    this.scmId = StringUtils.isNotBlank(scmId) ? scmId :
        UUID.randomUUID().toString();
    this.failCallsFrequency = Math.abs(failCallsFrequency);
    switch (this.failCallsFrequency) {
    case 0:
      LOG.debug("Set to no failure mode, all delete block calls will " +
          "succeed.");
      break;
    case 1:
      LOG.debug("Set to all failure mode. All delete block calls to SCM" +
          " will fail.");
      break;
    default:
      LOG.debug("Set to Mix mode, every {} -th call will fail",
          this.failCallsFrequency);
    }

  }

  /**
   * Returns Fake blocks to the BlockManager so we get blocks in the Database.
   * @param size - size of the block.
   * @param owner - String owner.
   * @param excludeList list of dns/pipelines to exclude
   * @return
   * @throws IOException
   */
  @Override
  public List<AllocatedBlock> allocateBlock(long size, int num,
      ReplicationConfig config,
      String owner, ExcludeList excludeList, String clientMachine)
      throws IOException {
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    Pipeline pipeline = createPipeline(datanodeDetails);
    long containerID = Time.monotonicNow();
    long localID = Time.monotonicNow();
    AllocatedBlock.Builder abb =
        new AllocatedBlock.Builder()
            .setContainerBlockID(new ContainerBlockID(containerID, localID))
            .setPipeline(pipeline);
    return Collections.singletonList(abb.build());
  }

  private Pipeline createPipeline(DatanodeDetails datanode) {
    List<DatanodeDetails> dns = new ArrayList<>();
    dns.add(datanode);
    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setNodes(dns)
        .build();
    return pipeline;
  }

  @Override
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {
    List<DeleteBlockGroupResult> results = new ArrayList<>();
    List<DeleteBlockResult> blockResultList = new ArrayList<>();
    Result result;
    for (BlockGroup keyBlocks : keyBlocksInfoList) {
      for (BlockID blockKey : keyBlocks.getBlockIDList()) {
        currentCall++;
        switch (this.failCallsFrequency) {
        case 0:
          result = success;
          numBlocksDeleted++;
          break;
        case 1:
          result = unknownFailure;
          break;
        default:
          if (currentCall % this.failCallsFrequency == 0) {
            result = unknownFailure;
          } else {
            result = success;
            numBlocksDeleted++;
          }
        }
        blockResultList.add(new DeleteBlockResult(blockKey, result));
      }
      results.add(new DeleteBlockGroupResult(keyBlocks.getGroupID(),
          blockResultList));
    }
    return results;
  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    ScmInfo.Builder builder =
        new ScmInfo.Builder()
            .setClusterId(clusterID)
            .setScmId(scmId);
    return builder.build();
  }

  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    return false;
  }

  @Override
  public List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) throws IOException {
    return null;
  }

  @Override
  public InnerNode getNetworkTopology() {
    InnerNode.Factory factory = InnerNodeImpl.FACTORY;
    InnerNode clusterTree = factory.newInnerNode("", "", null,
        NetConstants.ROOT_LEVEL, 1);
    return clusterTree;
  }

  /**
   * Return the number of blocks puesdo deleted by this testing client.
   */
  public int getNumberOfDeletedBlocks() {
    return numBlocksDeleted;
  }

  @Override
  public void close() throws IOException {

  }
}
