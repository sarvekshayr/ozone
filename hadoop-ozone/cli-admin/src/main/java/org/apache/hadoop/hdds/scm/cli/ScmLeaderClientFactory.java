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

package org.apache.hadoop.hdds.scm.cli;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB.ScmNodeTarget;

/**
 * Creates SCM clients pinned to the HA leader without transparent failover.
 */
public final class ScmLeaderClientFactory {

  private ScmLeaderClientFactory() {
  }

  /**
   * SCM client pinned to the current leader in HA, plus the resolved leader node when known.
   */
  public static final class LeaderPinnedClient implements AutoCloseable {
    private final ScmClient client;
    private final SCMNodeInfo leader;

    private LeaderPinnedClient(ScmClient client, SCMNodeInfo leader) {
      this.client = client;
      this.leader = leader;
    }

    public ScmClient getClient() {
      return client;
    }

    /**
     * @return the SCM leader in HA, or {@code null} in non-HA
     */
    public SCMNodeInfo getLeader() {
      return leader;
    }

    @Override
    public void close() throws IOException {
      client.close();
    }
  }

  /**
   * Returns an SCM client that talks only to the current leader.
   *
   * <p>In HA, {@code --scm} must target the leader when set. In non-HA, returns a normal SCM
   * client with no leader metadata.
   */
  public static LeaderPinnedClient createLeaderPinnedClient(ScmOption scmOption,
      OzoneConfiguration conf) throws IOException {
    if (HddsUtils.getScmServiceId(conf) == null) {
      return new LeaderPinnedClient(scmOption.createScmClient(conf), null);
    }

    List<SCMNodeInfo> nodes = SCMNodeInfo.buildNodeInfo(conf);
    ScmNodeTarget targetScmNode = new ScmNodeTarget();
    SCMNodeInfo leader;
    try (ScmClient rolesClient = scmOption.createScmClient(conf)) {
      leader = resolveLeader(rolesClient, nodes);
      if (leader == null) {
        throw new IOException("Could not determine SCM leader.");
      }
      validateLeaderTarget(scmOption.getScm(), leader, nodes);
      targetScmNode.setNodeId(leader.getNodeId());
    }
    return new LeaderPinnedClient(scmOption.createScmClient(conf, targetScmNode), leader);
  }

  /**
   * Returns the current SCM leader node in an HA cluster.
   */
  public static SCMNodeInfo resolveLeader(ScmClient scmClient, List<SCMNodeInfo> nodes)
      throws IOException {
    try {
      List<String> roles = scmClient.getScmRoles();
      for (String role : roles) {
        String[] parts = role.split(":");
        if (parts.length < 3 || !"LEADER".equalsIgnoreCase(parts[2])) {
          continue;
        }
        String leaderHost = parts[0];
        String leaderIp = parts.length >= 5 ? parts[4] : null;
        for (SCMNodeInfo node : nodes) {
          String nodeHost = node.getScmClientAddress().split(":", 2)[0];
          if (matchesAddress(leaderHost, nodeHost)
              || (leaderIp != null && !leaderIp.isEmpty() && matchesAddress(leaderIp, nodeHost))) {
            return node;
          }
        }
      }
      return null;
    } catch (IOException e) {
      throw new IOException("Could not determine SCM leader: " + e.getMessage(), e);
    }
  }

  private static void validateLeaderTarget(String scmAddress, SCMNodeInfo leader,
      List<SCMNodeInfo> nodes) throws IOException {
    if (StringUtils.isEmpty(scmAddress)) {
      return;
    }
    SCMNodeInfo targetNode = nodes.stream()
        .filter(node -> matchesAddress(node.getScmClientAddress(), scmAddress))
        .findFirst()
        .orElseThrow(() -> new IOException("Specified --scm address " + scmAddress
            + " does not match any SCM node."));
    if (!targetNode.getNodeId().equals(leader.getNodeId())) {
      throw new IOException("Container ID export must target the SCM leader. Current leader is "
          + leader.getNodeId() + " (" + leader.getScmClientAddress() + "), but --scm points to "
          + targetNode.getNodeId() + " (" + targetNode.getScmClientAddress() + ").");
    }
  }

  private static boolean matchesAddress(String address1, String address2) {
    if (address1.equalsIgnoreCase(address2)) {
      return true;
    }
    try {
      String[] parts1 = address1.split(":", 2);
      String[] parts2 = address2.split(":", 2);
      if (!parts1[0].equalsIgnoreCase(parts2[0])) {
        return false;
      }
      if (parts1.length > 1 && parts2.length > 1) {
        return parts1[1].equals(parts2[1]);
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
