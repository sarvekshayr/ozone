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

import java.io.PrintWriter;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.scm.cli.ScmLeaderClientFactory;
import org.apache.hadoop.hdds.scm.cli.ScmLeaderClientFactory.LeaderPinnedClient;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.container.export.ContainerExportStatus;
import picocli.CommandLine;

/**
 * Command to check status of a container ID export job on the SCM leader.
 */
@CommandLine.Command(
    name = "status",
    description = "Get status of a container ID export job on the SCM leader.")
public class ExportContainerStatus extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Option(
      names = {"--job-id"},
      required = true,
      description = "Export job ID returned by the export command")
  private String jobId;

  @Override
  public Void call() throws Exception {
    try (LeaderPinnedClient pinned = ScmLeaderClientFactory.createLeaderPinnedClient(scmOption, getOzoneConf())) {
      ExportContainerIDs.printScmLeader(out(), pinned.getLeader());
      printStatus(out(), pinned.getClient().getContainerIdExportStatus(jobId));
    }
    return null;
  }

  static void printStatus(PrintWriter out, ContainerExportStatus status) {
    out.printf("jobId=%s%n", status.getJobId());
    out.printf("state=%s%n", status.getState());
    if (status.getLifecycleState() != null) {
      out.printf("lifecycleState=%s%n", status.getLifecycleState());
    }
    if (status.getHealthState() != null) {
      out.printf("healthState=%s%n", status.getHealthState());
    }
    out.printf("totalRows=%,d%n", status.getTotalRows());
    out.printf("elapsedMs=%,d%n", status.getElapsedMs());
    if (status.getTarPath() != null) {
      out.printf("tarPath=%s%n", status.getTarPath());
      out.println("Delete the TAR file on the SCM leader manually when no longer needed.");
    }
    if (status.getErrorMessage() != null) {
      out.printf("errorMessage=%s%n", status.getErrorMessage());
    }
  }
}
