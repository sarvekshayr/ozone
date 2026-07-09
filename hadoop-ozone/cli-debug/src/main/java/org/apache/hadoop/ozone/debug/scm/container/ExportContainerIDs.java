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

package org.apache.hadoop.ozone.debug.scm.container;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ScmLeaderClientFactory;
import org.apache.hadoop.hdds.scm.cli.ScmLeaderClientFactory.LeaderPinnedClient;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits;
import org.apache.hadoop.hdds.scm.container.export.ContainerExportStatus;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.server.ServerUtils;
import picocli.CommandLine;

/**
 * Command to export container IDs to a TAR file on the SCM leader.
 *
 * <p>Health filters use Replication Manager's last-known {@code ContainerInfo.healthState}
 * and may be stale if RM has not evaluated a container yet.
 */
@CommandLine.Command(
    name = "export",
    description = "Export container IDs matching filters to a TAR on the SCM leader.",
    subcommands = {
        ExportContainerStatus.class
    })
public class ExportContainerIDs extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"--lifecycle-state"},
      description = "Container lifecycle state (OPEN, CLOSING, QUASI_CLOSED, CLOSED, DELETING, DELETED)")
  private HddsProtos.LifeCycleState lifeCycleState;

  @CommandLine.Option(
      names = {"--health-state"},
      description = "Container health state from Replication Manager (e.g. MISSING, EMPTY, UNDER_REPLICATED). "
          + "Reflects Replication Manager's last-known health and may be stale.")
  private ContainerHealthState healthState;

  @CommandLine.Option(
      names = {"--start"},
      description = "Container ID to start iteration from (inclusive, default 0)")
  private long startId;

  @CommandLine.Option(
      names = {"--count"},
      description = "Maximum number of container IDs to export (default: unlimited)")
  private long count;

  @CommandLine.Option(
      names = {"--page-size"},
      description = "IDs fetched per SCM read",
      defaultValue = "100000",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private int pageSize;

  @CommandLine.Option(
      names = {"--shard-size"},
      description = "IDs per TAR shard entry",
      defaultValue = "500000",
      showDefaultValue = CommandLine.Help.Visibility.ALWAYS)
  private int shardSize;

  @CommandLine.Option(
      names = {"-n", "--watch"},
      description = "Poll export status every N seconds until the job completes")
  private Long watchIntervalSec;

  @Override
  public Void call() throws Exception {
    if (healthState == null && lifeCycleState == null) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "At least one of --health-state or --lifecycle-state is required.");
    }
    if (watchIntervalSec != null && watchIntervalSec <= 0) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "--watch interval must be greater than 0.");
    }
    validateExportOptions();
    printExportNotes();

    ContainerID start = ContainerID.valueOf(startId);
    long maxRows = count > 0 ? count : 0;

    try (LeaderPinnedClient pinned = ScmLeaderClientFactory.createLeaderPinnedClient(
        scmOption, getOzoneConf())) {
      ScmClient client = pinned.getClient();
      printScmLeader(out(), pinned.getLeader());
      String jobId = client.submitContainerIdExport(start, lifeCycleState, healthState, maxRows, pageSize, shardSize);
      out().printf("Submitted container ID export job: %s%n", jobId);
      out().printf("Export directory on SCM leader: %s%n",
          resolveExportDirectory(getOzoneConf()));
      out().printf("Check status with: ozone debug scm container export status --job-id %s%n", jobId);
      out().println("Job status is kept in SCM leader memory only. If leadership changes or SCM restarts, " +
          "the job ID is no longer queryable; re-submit the export on the current leader. Completed TAR " +
          "files remain on the leader that ran the export");

      if (watchIntervalSec == null) {
        return null;
      }

      while (true) {
        ContainerExportStatus status = client.getContainerIdExportStatus(jobId);
        printProgress(status);
        if (status.isTerminal()) {
          if (status.getState() == ContainerExportStatus.State.FAILED) {
            throw new IOException(status.getErrorMessage() != null
                ? status.getErrorMessage()
                : "Export job failed");
          }
          printFinalStatus(status);
          return null;
        }
        TimeUnit.SECONDS.sleep(watchIntervalSec);
      }
    }
  }

  private static String resolveExportDirectory(OzoneConfiguration conf) {
    String configured = conf.getTrimmed(ScmConfigKeys.OZONE_SCM_CONTAINER_EXPORT_DIR, "");
    if (StringUtils.isNotEmpty(configured)) {
      return new java.io.File(configured).getAbsolutePath();
    }
    return new java.io.File(ServerUtils.getScmDbDir(conf),
        ContainerExportLimits.EXPORT_SUBDIR).getAbsolutePath();
  }

  static void printScmLeader(java.io.PrintWriter out, SCMNodeInfo leader) {
    if (leader != null) {
      out.printf("SCM leader: %s (%s)%n", leader.getNodeId(), leader.getScmClientAddress());
    }
  }

  private void printExportNotes() {
    if (healthState != null) {
      err().println("Note: --health-state reflects Replication Manager's last-known health and may be stale.");
    }
    if (healthState != null && lifeCycleState == null) {
      err().println("Note: Health-only export scans all containers (no health index). "
          + "Prefer --lifecycle-state when possible.");
    }
  }

  private void validateExportOptions() {
    if (startId < 0) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "--start must be greater than or equal to 0.");
    }
    if (count < 0) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "--count must be greater than or equal to 0.");
    }
    if (pageSize <= 0 || pageSize > ContainerExportLimits.MAX_PAGE_SIZE) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "--page-size must be between 1 and " + ContainerExportLimits.MAX_PAGE_SIZE + ".");
    }
    if (shardSize <= 0 || shardSize > ContainerExportLimits.MAX_SHARD_SIZE) {
      throw new CommandLine.ParameterException(spec.commandLine(),
          "--shard-size must be between 1 and " + ContainerExportLimits.MAX_SHARD_SIZE + ".");
    }
  }

  private void printFinalStatus(ContainerExportStatus status) {
    out().printf("Job %s %s: %,d rows exported in %,d ms%n",
        status.getJobId(), status.getState(),
        status.getTotalRows(), status.getElapsedMs());
    if (status.getTarPath() != null) {
      out().printf("TAR file on SCM leader: %s%n", status.getTarPath());
      out().println("Delete the TAR file on the SCM leader manually when no longer needed.");
    } else if (status.getState() == ContainerExportStatus.State.SUCCEEDED
        && status.getTotalRows() == 0) {
      out().println("No TAR file was created (zero matching container IDs).");
    }
    if (status.getErrorMessage() != null) {
      err().println(status.getErrorMessage());
    }
  }

  private void printProgress(ContainerExportStatus status) {
    err().printf("[export %s] %s: %,d rows (elapsed %,d ms)%n",
        status.getJobId(), status.getState(),
        status.getTotalRows(), status.getElapsedMs());
  }
}
