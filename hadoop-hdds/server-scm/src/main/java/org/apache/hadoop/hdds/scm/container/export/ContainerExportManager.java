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

import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_PAGE_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.DEFAULT_SHARD_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ExportLimits.EXPORT_SUBDIR;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
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
 * Manages asynchronous container ID export jobs on the SCM leader.
 *
 * <p>Health filters read {@link org.apache.hadoop.hdds.scm.container.ContainerInfo#getHealthState()}
 * as last written by Replication Manager; they are not recomputed during export and may be stale
 * if RM has not yet evaluated a container.
 *
 * <p>Job status is kept in memory only. On SCM restart or leader failover, in-flight jobs are lost
 * and the operator must re-submit on the new leader. Completed TAR files are kept on disk while their
 * job status remains in {@code jobTracker}; when a terminal job is evicted past {@code maxTerminalJobs},
 * its TAR is deleted as well. On startup, incomplete work ({@code {jobId}.in-progress} markers,
 * {@code export-{jobId}} work directories, and matching partial TAR files) is removed.
 */
public class ContainerExportManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerExportManager.class);

  static final String IN_PROGRESS_MARKER_SUFFIX = ".in-progress";
  static final String EXPORT_JOB_DIR_PREFIX = "export-";

  //TODO: make ozone.scm.container.export.max.terminal.jobs configurable.
  private static final int DEFAULT_MAX_TERMINAL_JOBS = 10;
  private static final long SHUTDOWN_TIMEOUT_MS = 5_000;

  private static final DateTimeFormatter METADATA_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter FILENAME_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  private final Map<ExportJob.Id, ExportJob> jobTracker = new ConcurrentHashMap<>();
  private final AtomicReference<ExportJob.Id> runningJobId = new AtomicReference<>();
  private final ExecutorService workerPool;
  private final ContainerManager containerManager;
  private final ExportMetrics metrics;
  private final BooleanSupplier isLeaderReady;
  private final String exportDirectory;
  private final int defaultShardSize;
  private final int defaultPageSize;
  private final int maxTerminalJobs;
  /** SCM node id, used in log messages and the export worker thread name. */
  private final String scmId;

  public ContainerExportManager(ContainerManager containerManager, BooleanSupplier isLeaderReady,
      OzoneConfiguration conf, String scmId) {
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.exportDirectory = Objects.requireNonNull(resolveExportDirectory(conf), "exportDirectory == null");
    this.scmId = Objects.requireNonNull(scmId, "scmId == null");
    this.defaultShardSize = DEFAULT_SHARD_SIZE;
    this.defaultPageSize = DEFAULT_PAGE_SIZE;
    this.maxTerminalJobs = DEFAULT_MAX_TERMINAL_JOBS;
    this.metrics = ExportMetrics.create();
    this.workerPool = newWorkerPool(this.scmId);
  }

  ContainerExportManager(ContainerManager containerManager, BooleanSupplier isLeaderReady,
      String exportDirectory, int defaultShardSize, int defaultPageSize, int maxTerminalJobs,
      String scmId) {
    this.containerManager = Objects.requireNonNull(containerManager, "containerManager == null");
    this.isLeaderReady = Objects.requireNonNull(isLeaderReady, "isLeaderReady == null");
    this.exportDirectory = Objects.requireNonNull(exportDirectory, "exportDirectory == null");
    this.scmId = Objects.requireNonNull(scmId, "scmId == null");
    this.defaultShardSize = defaultShardSize;
    this.defaultPageSize = defaultPageSize;
    this.maxTerminalJobs = maxTerminalJobs;
    this.metrics = null;
    this.workerPool = newWorkerPool(this.scmId);
  }

  private static ExecutorService newWorkerPool(String scmId) {
    return Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, scmId + "-ContainerExportWorker");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Initializes the export directory. Must be called once before submitting jobs.
   */
  public void start() throws IOException {
    Files.createDirectories(Paths.get(exportDirectory));
    cleanupOrphanedExportArtifacts();
    LOG.info("{}: ContainerExportManager started (dir={}, defaultShardSize={}, defaultPageSize={}, maxTerminalJobs={})",
        scmId, exportDirectory, defaultShardSize, defaultPageSize, maxTerminalJobs);
  }

  // TODO: make ozone.scm.container.export.dir configurable
  // Set path separate from scm.db.dirs to avoid large export TAR I/O 
  // contending with SCM metadata DB access as the disk fills.
  private static String resolveExportDirectory(OzoneConfiguration conf) {
    File scmDbDir = ServerUtils.getScmDbDir(conf);
    return new File(scmDbDir, EXPORT_SUBDIR).getAbsolutePath();
  }

  /**
   * Submit a container ID export job on the SCM leader.
   * Batch sizing is described by {@link ExportSizing}.
   *
   * @return job id, or {@code null} if not leader or another export is already running
   */
  public ExportJob.Id submitJob(ContainerID start, LifeCycleState lifeCycleState,
      ContainerHealthState healthState, long maxRows, int pageSize, int shardSize) {
    if (!isLeaderReady.getAsBoolean()) {
      return null;
    }
    if (lifeCycleState == null && healthState == null) {
      throw new IllegalArgumentException("At least one of healthState or lifecycleState filter is required.");
    }
    validateRequest(start);
    ExportSizing.validate(maxRows, pageSize, shardSize);
    ExportSizing sizing = ExportSizing.resolve(maxRows, pageSize, shardSize, defaultPageSize, defaultShardSize);

    ExportJob.Id jobId = ExportJob.Id.newId();
    if (!runningJobId.compareAndSet(null, jobId)) {
      return null;
    }

    ExportScope scope = ExportScope.of(lifeCycleState, healthState);
    Instant now = Instant.now();
    String metadataTimestamp = METADATA_TIMESTAMP_FORMAT.format(now);
    String fileTimestamp = FILENAME_TIMESTAMP_FORMAT.format(now);
    String tarFileName = String.format("container-ids-%s-%s-%s.tar", scope.getValue(), fileTimestamp, jobId.getValue());
    String tarPath = exportDirectory + File.separator + tarFileName;

    ExportJob job = new ExportJob(jobId, scope, metadataTimestamp, tarPath, start, sizing);

    evictOldTerminalJobs();
    jobTracker.put(jobId, job);

    if (metrics != null) {
      metrics.incrExportJobsSubmitted();
    }

    workerPool.submit(() -> executeExport(job));
    LOG.info("{}: submitted container ID export job {} (scope={}, start={}, maxRows={}, pageSize={}, shardSize={})",
        scmId, jobId, scope, start, sizing.getMaxRows(), sizing.getPageSize(), sizing.getShardSize());
    return jobId;
  }

  private static void validateRequest(ContainerID start) {
    if (start != null && start.getProtobuf().getId() < 0) {
      throw new IllegalArgumentException("start container ID must be non-negative: " + start.getProtobuf().getId());
    }
  }

  public ExportJob.Status getExportStatus(ExportJob.Id jobId) {
    ExportJob job = jobTracker.get(jobId);
    return job != null ? job.toStatus() : null;
  }

  public void shutdown() {
    LOG.info("{}: shutting down ContainerExportManager", scmId);
    workerPool.shutdownNow();
    try {
      if (!workerPool.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        LOG.warn("{}: timed out waiting for export worker shutdown", scmId);
      }
    } catch (InterruptedException e) {
      LOG.warn("{}: interrupted waiting for export worker shutdown", scmId);
      Thread.currentThread().interrupt();
    }
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  Map<ExportJob.Id, ExportJob> getJobTracker() {
    return jobTracker;
  }

  private void evictOldTerminalJobs() {
    List<Map.Entry<ExportJob.Id, ExportJob>> terminalJobs = jobTracker.entrySet().stream()
        .filter(e -> e.getValue().getExecutionState() != ExportJob.ExecutionState.RUNNING)
        .sorted(Comparator.comparingLong(e -> e.getValue().getEndTimeNs()))
        .collect(Collectors.toList());
    int excess = terminalJobs.size() - maxTerminalJobs;
    for (int i = 0; i < excess; i++) {
      ExportJob evicted = terminalJobs.get(i).getValue();
      deleteExportTar(evicted);
      jobTracker.remove(evicted.getId());
    }
  }

  private void deleteExportTar(ExportJob job) {
    String tarPath = job.getTarPath();
    if (tarPath == null) {
      return;
    }
    File tar = new File(tarPath);
    if (tar.isFile() && FileUtils.deleteQuietly(tar)) {
      LOG.debug("Removed container export TAR for evicted job {}: {}", job.getId(), tar.getName());
    }
  }

  private void cleanupOrphanedExportArtifacts() {
    File exportDir = new File(exportDirectory);
    File[] children = exportDir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isFile() && child.getName().endsWith(IN_PROGRESS_MARKER_SUFFIX)) {
        String jobId = child.getName().substring(
            0, child.getName().length() - IN_PROGRESS_MARKER_SUFFIX.length());
        if (isUuidDirectoryName(jobId)) {
          removeIncompleteExportArtifacts(jobId);
        }
      }
    }
    for (File child : children) {
      if (child.isDirectory()) {
        String jobId = jobIdFromExportDirName(child.getName());
        if (jobId != null) {
          removeIncompleteExportArtifacts(jobId);
        }
      }
    }
  }

  private static String exportJobDirName(String jobId) {
    return EXPORT_JOB_DIR_PREFIX + jobId;
  }

  private static String jobIdFromExportDirName(String dirName) {
    if (!dirName.startsWith(EXPORT_JOB_DIR_PREFIX)) {
      return null;
    }
    String jobId = dirName.substring(EXPORT_JOB_DIR_PREFIX.length());
    return isUuidDirectoryName(jobId) ? jobId : null;
  }

  private void removeIncompleteExportArtifacts(String jobId) {
    LOG.info("Removing incomplete container export artifacts for job {}", jobId);
    FileUtils.deleteQuietly(inProgressMarkerFile(jobId));
    File tar = findTarForJobId(jobId);
    if (tar != null) {
      FileUtils.deleteQuietly(tar);
      LOG.info("Removed incomplete container export TAR for job {}: {}", jobId, tar.getName());
    }
    File jobWorkDir = new File(exportDirectory, exportJobDirName(jobId));
    if (jobWorkDir.isDirectory()) {
      FileUtils.deleteQuietly(jobWorkDir);
      LOG.info("Removed orphaned container export work directory: {}",
          jobWorkDir.getAbsolutePath());
    }
  }

  private File findTarForJobId(String jobId) {
    File exportDir = new File(exportDirectory);
    File[] matches = exportDir.listFiles(
        (dir, fileName) -> fileName.endsWith("-" + jobId + ".tar"));
    if (matches == null || matches.length == 0) {
      return null;
    }
    return matches[0];
  }

  private File inProgressMarkerFile(String jobId) {
    return new File(exportDirectory, jobId + IN_PROGRESS_MARKER_SUFFIX);
  }

  private void markExportInProgress(String jobId) throws IOException {
    Files.createFile(inProgressMarkerFile(jobId).toPath());
  }

  private void clearExportInProgress(String jobId) {
    FileUtils.deleteQuietly(inProgressMarkerFile(jobId));
  }

  private static boolean isUuidDirectoryName(String directoryName) {
    try {
      return directoryName.equals(UUID.fromString(directoryName).toString());
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private void executeExport(ExportJob job) {
    String jobIdValue = job.getId().getValue();
    Path jobDir = Paths.get(exportDirectory, exportJobDirName(jobIdValue));
    Path workDir = jobDir.resolve("work");
    File tarFile = new File(job.getTarPath());
    job.setStartTimeNs(System.nanoTime());
    boolean succeeded = false;
    Archiver.AppendableTar appendableTar = null;

    try {
      Files.createDirectories(workDir);
      job.setExecutionState(ExportJob.ExecutionState.RUNNING);
      markExportInProgress(jobIdValue);

      ContainerID cursor = job.getStartContainerId();
      int fileIndex = 1;
      long totalRows = 0;
      long recordsInCurrentFile = 0;
      BufferedWriter writer = null;
      Path currentShardPath = null;
      // Pre-allocated buffer: ~12 chars per ID (up to 20 digits + newline) per page.
      StringBuilder buf = new StringBuilder(job.getPageSize() * 12);

      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Export job " + jobIdValue + " cancelled");
          }
          if (!isLeaderReady.getAsBoolean()) {
            throw new IOException(scmId + ": SCM lost leadership during export job " + jobIdValue);
          }

          int fetchCount = job.getPageSize();
          if (job.getMaxRows() > 0) {
            long remaining = job.getMaxRows() - totalRows;
            if (remaining <= 0) {
              break;
            }
            fetchCount = (int) Math.min(fetchCount, remaining);
          }

          List<ContainerID> page = containerManager.getContainerIDs(
              cursor, fetchCount, job.getLifeCycleState(), job.getHealthState());
          if (page.isEmpty()) {
            break;
          }

          for (ContainerID containerId : page) {
            if (recordsInCurrentFile == 0) {
              writer = closeWriter(writer);
              currentShardPath = workDir.resolve(job.shardFileName(fileIndex));
              writer = Files.newBufferedWriter(currentShardPath, StandardCharsets.UTF_8);
              job.writeMetadataHeader(writer, fileIndex, containerId);
              LOG.info("{}: export job {} created shard part{}", scmId, jobIdValue, fileIndex);
            }

            buf.append(containerId.getProtobuf().getId()).append('\n');
            totalRows++;
            recordsInCurrentFile++;
            job.setTotalRows(totalRows);

            if (recordsInCurrentFile >= job.getShardSize()) {
              writer.write(buf.toString());
              buf.setLength(0);
              writer = closeWriter(writer);
              if (appendableTar == null) {
                appendableTar = Archiver.openForAppend(tarFile);
              }
              appendShardToTar(appendableTar, currentShardPath, job, fileIndex);
              currentShardPath = null;
              recordsInCurrentFile = 0;
              fileIndex++;
            }
          }

          // Flush the batch buffer at the end of each page.
          if (buf.length() > 0 && writer != null) {
            writer.write(buf.toString());
            buf.setLength(0);
          }

          cursor = ContainerID.valueOf(
              page.get(page.size() - 1).getProtobuf().getId() + 1);
        }

        writer = closeWriter(writer);
        if (totalRows == 0) {
          clearExportInProgress(jobIdValue);
          FileUtils.deleteQuietly(workDir.toFile());
          FileUtils.deleteQuietly(jobDir.toFile());
          job.setExecutionState(ExportJob.ExecutionState.SUCCEEDED);
          job.setTarPath(null);
          succeeded = true;
          LOG.info("{}: export job {} completed with zero matching containers", scmId, jobIdValue);
          return;
        }

        if (currentShardPath != null) {
          if (appendableTar == null) {
            appendableTar = Archiver.openForAppend(tarFile);
          }
          appendShardToTar(appendableTar, currentShardPath, job, fileIndex);
        }

        FileUtils.deleteQuietly(workDir.toFile());
        FileUtils.deleteQuietly(jobDir.toFile());
        job.setExecutionState(ExportJob.ExecutionState.SUCCEEDED);
        succeeded = true;
        LOG.info("{}: export job {} completed ({} rows, tar={}).",
            scmId, jobIdValue, totalRows, tarFile.getAbsolutePath());
      } finally {
        closeWriter(writer);
        if (appendableTar != null) {
          appendableTar.close();
          if (succeeded) {
            clearExportInProgress(jobIdValue);
          }
        }
      }
    } catch (InterruptedException e) {
      job.setExecutionState(ExportJob.ExecutionState.FAILED);
      job.setErrorMessage(e.getMessage());
      cleanupFailedArtifacts(jobDir, tarFile, jobIdValue);
      LOG.info("{}: export job {} was cancelled", scmId, jobIdValue);
      Thread.currentThread().interrupt();
    } catch (IOException | RuntimeException e) {
      succeeded = false;
      job.setExecutionState(ExportJob.ExecutionState.FAILED);
      job.setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
      cleanupFailedArtifacts(jobDir, tarFile, jobIdValue);
      LOG.error("{}: export job {} failed", scmId, jobIdValue, e);
    } finally {
      runningJobId.compareAndSet(job.getId(), null);
      if (metrics != null) {
        if (succeeded) {
          metrics.incrExportJobsSucceeded();
          long bytesWritten = tarFile.isFile() ? tarFile.length() : 0L;
          metrics.recordLastSuccessfulExport(job.getTotalRows(), bytesWritten);
        } else if (job.getExecutionState() == ExportJob.ExecutionState.FAILED) {
          metrics.incrExportJobsFailed();
        }
      }
      if (job.getExecutionState() == ExportJob.ExecutionState.SUCCEEDED
          || job.getExecutionState() == ExportJob.ExecutionState.FAILED) {
        evictOldTerminalJobs();
      }
    }
  }

  private void appendShardToTar(Archiver.AppendableTar tar, Path shardPath, ExportJob job, int partIndex)
      throws IOException {
    tar.appendFile(shardPath.toFile(), job.shardEntryName(partIndex));
    FileUtils.deleteQuietly(shardPath.toFile());
  }

  /** Remove partial work artifacts after a failed or cancelled export. */
  private void cleanupFailedArtifacts(Path jobDir, File tarFile, String jobId) {
    if (jobDir != null) {
      FileUtils.deleteQuietly(jobDir.toFile());
    }
    if (tarFile != null) {
      FileUtils.deleteQuietly(tarFile);
    }
    clearExportInProgress(jobId);
  }

  private static BufferedWriter closeWriter(BufferedWriter writer) throws IOException {
    if (writer != null) {
      writer.flush();
      writer.close();
    }
    return null;
  }
}
