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
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.export.ContainerExportStatus;
import picocli.CommandLine;

/**
 * Command to export container IDs to a TAR file on SCM.
 */
@CommandLine.Command(
    name = "export",
    description = "Export container IDs matching filters to a TAR file on SCM.",
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
      description = "Container health state (e.g. MISSING, EMPTY, UNDER_REPLICATED)")
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

    ContainerID start = ContainerID.valueOf(startId);
    long maxRows = count > 0 ? count : 0;

    try (ContainerOperationClient client = new ContainerOperationClient(getOzoneConf())) {
      String jobId = client.submitContainerIdExport(start, lifeCycleState, healthState, maxRows);
      ContainerExportStatus initialStatus = client.getContainerIdExportStatus(jobId);
      out().printf("Submitted container ID export job: %s%n", jobId);
      printExportDir(initialStatus);
      out().printf("Check status with: ozone debug scm container export status --job-id %s%n", jobId);

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

  private void printExportDir(ContainerExportStatus status) {
    if (status.getExportDir() != null) {
      out().printf("Export directory on SCM leader: %s%n", status.getExportDir());
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
