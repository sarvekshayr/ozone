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

package org.apache.hadoop.hdds.scm.container.export;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.Archiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages asynchronous container ID export jobs on SCM leader.
 */
public class ContainerExportManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerExportManager.class);

  private static final DateTimeFormatter METADATA_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter FILENAME_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  @VisibleForTesting
  static final String EXPORT_SUBDIR = "exports";
  @VisibleForTesting
  static final int SHARD_RECORDS = 500_000;
  @VisibleForTesting
  static final int PAGE_SIZE = 10_000;

  private final Map<String, ExportJob> jobTracker = new ConcurrentHashMap<>();
  private final ExecutorService workerPool;
  private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
  private final ContainerManager containerManager;
  private final String exportDirectory;
  private final int shardRecords;
  private final int pageSize;
  private final Object submitLock = new Object();

  public ContainerExportManager(ContainerManager containerManager,
                                OzoneConfiguration conf) {
    this(containerManager, resolveExportDirectory(conf),
        SHARD_RECORDS, PAGE_SIZE);
  }

  @VisibleForTesting
  ContainerExportManager(ContainerManager containerManager,
      String exportDirectory, int shardRecords, int pageSize) {
    this.containerManager = containerManager;
    this.exportDirectory = exportDirectory;
    this.shardRecords = shardRecords;
    this.pageSize = pageSize;
    this.workerPool = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "ContainerExportWorker");
      t.setDaemon(true);
      return t;
    });

    try {
      Files.createDirectories(Paths.get(exportDirectory));
    } catch (IOException e) {
      LOG.error("Failed to create export directory: {}", exportDirectory, e);
    }
    LOG.info("ContainerExportManager initialized (dir={}, shardRecords={}, pageSize={})",
        exportDirectory, shardRecords, pageSize);
  }

  private static String resolveExportDirectory(OzoneConfiguration conf) {
    File scmDbDir = ServerUtils.getScmDbDir(conf);
    return new File(scmDbDir, EXPORT_SUBDIR).getAbsolutePath();
  }

  /**
   * Submit a container ID export job.
   *
   * @param start optional inclusive start container ID (0 for beginning)
   * @param lifeCycleState optional lifecycle filter
   * @param healthState optional health filter
   * @param maxRows optional row limit (0 = unlimited)
   * @return job id
   */
  public String submitJob(ContainerID start, LifeCycleState lifeCycleState,
      ContainerHealthState healthState, long maxRows) {
    if (lifeCycleState == null && healthState == null) {
      throw new IllegalArgumentException("At least one of healthState or lifecycleState filter is required.");
    }
    String jobId = UUID.randomUUID().toString();
    String scope = buildScope(lifeCycleState, healthState);
    Instant now = Instant.now();
    String metadataTimestamp = METADATA_TIMESTAMP_FORMAT.format(now);
    String fileTimestamp = FILENAME_TIMESTAMP_FORMAT.format(now);
    String tarFileName = String.format("container-ids-%s-%s-%s.tar", scope, fileTimestamp, jobId);
    String tarPath = exportDirectory + File.separator + tarFileName;

    ExportJob job = new ExportJob(jobId, scope, metadataTimestamp, tarPath, exportDirectory,
        start, lifeCycleState, healthState, maxRows);

    synchronized (submitLock) {
      boolean exportInProgress = jobTracker.values().stream()
          .anyMatch(j -> j.getState() == ContainerExportStatus.State.RUNNING);
      if (exportInProgress) {
        throw new IllegalStateException("Another container ID export is already running.");
      }
      jobTracker.put(jobId, job);
    }

    Future<?> future = workerPool.submit(() -> executeExport(job));
    runningTasks.put(jobId, future);
    LOG.info("Submitted container ID export job {} (scope={}, start={}, maxRows={})", jobId, scope, start, maxRows);
    return jobId;
  }

  public ContainerExportStatus getJobStatus(String jobId) {
    ExportJob job = jobTracker.get(jobId);
    if (job == null) {
      return null;
    }
    return job.toStatus();
  }

  public void shutdown() {
    LOG.info("Shutting down ContainerExportManager");
    workerPool.shutdownNow();
    try {
      workerPool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Timeout waiting for export worker shutdown", e);
      Thread.currentThread().interrupt();
    }
    runningTasks.clear();
  }

  @VisibleForTesting
  String getExportDirectory() {
    return exportDirectory;
  }

  @VisibleForTesting
  Map<String, ExportJob> getJobTracker() {
    return jobTracker;
  }

  private void executeExport(ExportJob job) {
    Path jobDir = Paths.get(exportDirectory, job.getJobId());
    Path workDir = jobDir.resolve("work");
    File tarFile = new File(job.getTarPath());
    long startTimeMs = System.currentTimeMillis();
    job.setStartTimeMs(startTimeMs);

    try {
      Files.createDirectories(workDir);
      job.setState(ContainerExportStatus.State.RUNNING);

      ContainerID cursor = job.getStartContainerId();
      int fileIndex = 1;
      long totalRows = 0;
      long recordsInCurrentFile = 0;
      BufferedWriter writer = null;
      Path currentShardPath = null;

      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Job cancelled");
          }

          int pageCount = pageSize;
          if (job.getMaxRows() > 0) {
            long remaining = job.getMaxRows() - totalRows;
            if (remaining <= 0) {
              break;
            }
            pageCount = (int) Math.min(pageCount, remaining);
          }

          List<ContainerID> page = containerManager.getContainerIDs(
              cursor, pageCount, job.getLifeCycleState(), job.getHealthState());
          if (page.isEmpty()) {
            break;
          }

          for (ContainerID containerId : page) {
            if (recordsInCurrentFile == 0) {
              writer = closeWriter(writer);
              currentShardPath = workDir.resolve(shardFileName(job, fileIndex));
              writer = Files.newBufferedWriter(currentShardPath, StandardCharsets.UTF_8);
              writeMetadataHeader(writer, job, fileIndex, containerId);
              LOG.info("Export job {} created shard part{}", job.getJobId(), fileIndex);
            }

            writer.write(Long.toString(containerId.getId()));
            writer.newLine();
            totalRows++;
            recordsInCurrentFile++;
            job.setTotalRows(totalRows);

            if (recordsInCurrentFile >= shardRecords) {
              writer = closeWriter(writer);
              appendShardToTar(tarFile, currentShardPath, shardEntryName(job, fileIndex));
              currentShardPath = null;
              recordsInCurrentFile = 0;
              fileIndex++;
            }
          }

          cursor = ContainerID.valueOf(page.get(page.size() - 1).getId() + 1);
          if (writer != null && totalRows % 10_000 == 0) {
            writer.flush();
          }
        }

        writer = closeWriter(writer);
        if (totalRows == 0) {
          FileUtils.deleteQuietly(workDir.toFile());
          FileUtils.deleteQuietly(jobDir.toFile());
          job.setState(ContainerExportStatus.State.SUCCEEDED);
          job.setTarPath(null);
          LOG.info("Export job {} completed with zero matching containers", job.getJobId());
          return;
        }

        if (currentShardPath != null) {
          appendShardToTar(tarFile, currentShardPath, shardEntryName(job, fileIndex));
        }

        FileUtils.deleteQuietly(workDir.toFile());
        FileUtils.deleteQuietly(jobDir.toFile());
        job.setState(ContainerExportStatus.State.SUCCEEDED);
        LOG.info("Export job {} completed ({} rows, tar={}). "
                + "Delete the TAR file manually on the SCM leader when no longer needed.",
            job.getJobId(), totalRows, tarFile.getAbsolutePath());
      } finally {
        closeWriter(writer);
      }
    } catch (InterruptedException e) {
      job.setState(ContainerExportStatus.State.FAILED);
      job.setErrorMessage("Job was cancelled");
      cleanupFailedArtifacts(jobDir, tarFile);
      LOG.info("Export job {} was cancelled", job.getJobId());
      Thread.currentThread().interrupt();
    } catch (IOException | RuntimeException e) {
      job.setState(ContainerExportStatus.State.FAILED);
      job.setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
      cleanupFailedArtifacts(jobDir, tarFile);
      LOG.error("Export job {} failed", job.getJobId(), e);
    } finally {
      runningTasks.remove(job.getJobId());
    }
  }

  private static String shardFileName(ExportJob job, int partIndex) {
    return String.format("container-ids-%s-%s-part%03d.txt",
        job.getScope(), job.getTimestamp(), partIndex);
  }

  private static String shardEntryName(ExportJob job, int partIndex) {
    return shardFileName(job, partIndex);
  }

  private static void appendShardToTar(File tarFile, Path shardPath, String entryName)
      throws IOException {
    Archiver.appendFile(tarFile, shardPath.toFile(), entryName);
    FileUtils.deleteQuietly(shardPath.toFile());
  }

  /** Remove partial work artifacts after a failed or cancelled export. */
  private void cleanupFailedArtifacts(Path jobDir, File tarFile) {
    if (jobDir != null) {
      FileUtils.deleteQuietly(jobDir.toFile());
    }
    if (tarFile != null) {
      FileUtils.deleteQuietly(tarFile);
    }
  }

  private static BufferedWriter closeWriter(BufferedWriter writer) throws IOException {
    if (writer != null) {
      writer.flush();
      writer.close();
    }
    return null;
  }

  private static void writeMetadataHeader(BufferedWriter writer, ExportJob job,
      int partNumber, ContainerID shardStartContainerId) throws IOException {
    writer.newLine();
    writer.write("# jobId=" + job.getJobId());
    writer.newLine();
    writer.write("# timestamp=" + job.getTimestamp());
    writer.newLine();
    if (job.getHealthState() != null) {
      writer.write("# healthState=" + job.getHealthState().name());
      writer.newLine();
    }
    if (job.getLifeCycleState() != null) {
      writer.write("# lifecycleState=" + job.getLifeCycleState().name());
      writer.newLine();
    }
    writer.write("# startContainerId=" + shardStartContainerId.getId());
    writer.newLine();
    writer.write("# part=" + partNumber);
    writer.newLine();
    writer.write("# format=container-id-per-line");
    writer.newLine();
    writer.newLine();
  }

  static String buildScope(LifeCycleState lifeCycleState,
      ContainerHealthState healthState) {
    List<String> parts = new ArrayList<>(2);
    if (healthState != null) {
      parts.add("health-" + healthState.name());
    }
    if (lifeCycleState != null) {
      parts.add("lifecycle-" + lifeCycleState.name());
    }
    return String.join("_", parts);
  }

  static final class ExportJob {
    private final String jobId;
    private final String scope;
    private final String timestamp;
    private final ContainerID startContainerId;
    private final LifeCycleState lifeCycleState;
    private final ContainerHealthState healthState;
    private final long maxRows;
    private final String exportDir;
    private volatile String tarPath;
    private volatile ContainerExportStatus.State state =
        ContainerExportStatus.State.RUNNING;
    private volatile long totalRows;
    private volatile long startTimeMs;
    private volatile long endTimeMs;
    private volatile String errorMessage;

    @SuppressWarnings("checkstyle:ParameterNumber")
    ExportJob(String jobId, String scope, String timestamp, String tarPath,
        String exportDir, ContainerID startContainerId, LifeCycleState lifeCycleState,
        ContainerHealthState healthState, long maxRows) {
      this.jobId = jobId;
      this.scope = scope;
      this.timestamp = timestamp;
      this.tarPath = tarPath;
      this.exportDir = exportDir;
      this.startContainerId = startContainerId != null
          ? startContainerId : ContainerID.valueOf(0);
      this.lifeCycleState = lifeCycleState;
      this.healthState = healthState;
      this.maxRows = maxRows;
    }

    String getJobId() {
      return jobId;
    }

    String getScope() {
      return scope;
    }

    String getTimestamp() {
      return timestamp;
    }

    ContainerID getStartContainerId() {
      return startContainerId;
    }

    LifeCycleState getLifeCycleState() {
      return lifeCycleState;
    }

    ContainerHealthState getHealthState() {
      return healthState;
    }

    long getMaxRows() {
      return maxRows;
    }

    ContainerExportStatus.State getState() {
      return state;
    }

    void setState(ContainerExportStatus.State newState) {
      this.state = newState;
      if ((newState == ContainerExportStatus.State.SUCCEEDED
          || newState == ContainerExportStatus.State.FAILED)
          && endTimeMs == 0) {
        this.endTimeMs = System.currentTimeMillis();
      }
    }

    void setTotalRows(long rows) {
      this.totalRows = rows;
    }

    void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }

    void setErrorMessage(String message) {
      this.errorMessage = message;
    }

    String getTarPath() {
      return tarPath;
    }

    void setTarPath(String path) {
      this.tarPath = path;
    }

    ContainerExportStatus toStatus() {
      long elapsed;
      if (startTimeMs <= 0) {
        elapsed = 0;
      } else if (endTimeMs > 0) {
        elapsed = endTimeMs - startTimeMs;
      } else {
        elapsed = System.currentTimeMillis() - startTimeMs;
      }
      String health = healthState != null ? healthState.name() : null;
      String lifecycle = lifeCycleState != null ? lifeCycleState.name() : null;
      return new ContainerExportStatus(jobId, state, totalRows, elapsed,
          tarPath, errorMessage, health, lifecycle, exportDir);
    }
  }
}
