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

import static org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits.DEFAULT_PAGE_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits.DEFAULT_SHARD_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits.EXPORT_SUBDIR;
import static org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits.MAX_PAGE_SIZE;
import static org.apache.hadoop.hdds.scm.container.export.ContainerExportLimits.MAX_SHARD_SIZE;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * its TAR is deleted as well. On startup, incomplete work ({@code {jobId}.inprogress} markers, UUID work
 * directories, and matching partial TAR files) is removed.
 */
public class ContainerExportManager {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerExportManager.class);

  private static final String INPROGRESS_MARKER_SUFFIX = ".inprogress";

  //TODO: make ozone.scm.container.export.max.terminal.jobs configurable.
  private static final int DEFAULT_MAX_TERMINAL_JOBS = 10;

  private static final DateTimeFormatter METADATA_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter FILENAME_TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

  private final Map<String, ExportJob> jobTracker = new ConcurrentHashMap<>();
  private final ExecutorService workerPool;
  private final ContainerManager containerManager;
  private final ContainerExportMetrics metrics;
  private final BooleanSupplier isLeaderReady;
  private final String exportDirectory;
  private final int defaultShardSize;
  private final int defaultPageSize;
  private final int maxTerminalJobs;
  private final Object submitLock = new Object();

  public ContainerExportManager(ContainerManager containerManager,
      OzoneConfiguration conf, BooleanSupplier isLeaderReady) {
    this(containerManager, resolveExportDirectory(conf), DEFAULT_SHARD_SIZE, DEFAULT_PAGE_SIZE,
        DEFAULT_MAX_TERMINAL_JOBS, isLeaderReady, ContainerExportMetrics.create());
  }

  ContainerExportManager(ContainerManager containerManager, String exportDirectory,
      int defaultShardSize, int defaultPageSize, int maxTerminalJobs,
      BooleanSupplier isLeaderReady, ContainerExportMetrics metrics) {
    this.containerManager = containerManager;
    this.exportDirectory = exportDirectory;
    this.defaultShardSize = defaultShardSize;
    this.defaultPageSize = defaultPageSize;
    this.maxTerminalJobs = maxTerminalJobs;
    this.isLeaderReady = isLeaderReady;
    this.metrics = metrics;
    this.workerPool = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "ContainerExportWorker");
      t.setDaemon(true);
      return t;
    });

    try {
      Files.createDirectories(Paths.get(exportDirectory));
      cleanupOrphanedExportArtifacts();
    } catch (IOException e) {
      LOG.error("Failed to initialize export directory: {}", exportDirectory, e);
    }
    LOG.info("ContainerExportManager initialized (dir={}, defaultShardSize={}, defaultPageSize={}, maxTerminalJobs={})",
        exportDirectory, defaultShardSize, defaultPageSize, maxTerminalJobs);
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
   */
  public String submitJob(ContainerID start, LifeCycleState lifeCycleState,
      ContainerHealthState healthState, long maxRows, int pageSize, int shardSize) {
    if (!isLeaderReady.getAsBoolean()) {
      throw new IllegalStateException(
          "Container ID export must be submitted on the SCM leader.");
    }
    if (lifeCycleState == null && healthState == null) {
      throw new IllegalArgumentException("At least one of healthState or lifecycleState filter is required.");
    }
    validateRequest(start, maxRows, pageSize, shardSize);
    int resolvedPageSize = pageSize > 0 ? pageSize : defaultPageSize;
    int resolvedShardSize = shardSize > 0 ? shardSize : defaultShardSize;

    String jobId = UUID.randomUUID().toString();
    String scope = buildScope(lifeCycleState, healthState);
    Instant now = Instant.now();
    String metadataTimestamp = METADATA_TIMESTAMP_FORMAT.format(now);
    String fileTimestamp = FILENAME_TIMESTAMP_FORMAT.format(now);
    String tarFileName = String.format("container-ids-%s-%s-%s.tar", scope, fileTimestamp, jobId);
    String tarPath = exportDirectory + File.separator + tarFileName;

    ExportJob job = new ExportJob(jobId, scope, metadataTimestamp, tarPath,
        start, lifeCycleState, healthState, maxRows, resolvedPageSize, resolvedShardSize);

    synchronized (submitLock) {
      boolean exportInProgress = jobTracker.values().stream()
          .anyMatch(j -> j.getState() == ContainerExportStatus.State.RUNNING);
      if (exportInProgress) {
        throw new IllegalStateException("Another container ID export is already running.");
      }
      evictOldTerminalJobs();
      jobTracker.put(jobId, job);
    }

    if (metrics != null) {
      metrics.incrExportJobsSubmitted();
    }

    workerPool.submit(() -> executeExport(job));
    LOG.info("Submitted container ID export job {} (scope={}, start={}, maxRows={}, pageSize={}, shardSize={})",
        jobId, scope, start, maxRows, resolvedPageSize, resolvedShardSize);
    return jobId;
  }

  private static void validateRequest(ContainerID start, long maxRows, int pageSize, int shardSize) {
    if (start != null && start.getProtobuf().getId() < 0) {
      throw new IllegalArgumentException("start container ID must be non-negative.");
    }
    if (maxRows < 0) {
      throw new IllegalArgumentException("maxRows must be non-negative.");
    }
    if (pageSize < 0 || pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException("pageSize must be between 0 and " + MAX_PAGE_SIZE + ".");
    }
    if (shardSize < 0 || shardSize > MAX_SHARD_SIZE) {
      throw new IllegalArgumentException("shardSize must be between 0 and " + MAX_SHARD_SIZE + ".");
    }
  }

  public ContainerExportStatus getJobStatus(String jobId) {
    ExportJob job = jobTracker.get(jobId);
    return job != null ? job.toStatus() : null;
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
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  Map<String, ExportJob> getJobTracker() {
    return jobTracker;
  }

  private void evictOldTerminalJobs() {
    List<Map.Entry<String, ExportJob>> terminalJobs = jobTracker.entrySet().stream()
        .filter(e -> e.getValue().getState() != ContainerExportStatus.State.RUNNING)
        .sorted(Comparator.comparingLong(e -> e.getValue().getEndTimeMs()))
        .collect(Collectors.toList());
    int excess = terminalJobs.size() - maxTerminalJobs;
    for (int i = 0; i < excess; i++) {
      Map.Entry<String, ExportJob> evicted = terminalJobs.get(i);
      deleteExportTar(evicted.getValue());
      jobTracker.remove(evicted.getKey());
    }
  }

  private void deleteExportTar(ExportJob job) {
    String tarPath = job.getTarPath();
    if (tarPath == null) {
      return;
    }
    File tar = new File(tarPath);
    if (tar.isFile() && FileUtils.deleteQuietly(tar)) {
      LOG.debug("Removed container export TAR for evicted job {}: {}", job.getJobId(), tar.getName());
    }
  }

  private void cleanupOrphanedExportArtifacts() throws IOException {
    File exportDir = new File(exportDirectory);
    File[] children = exportDir.listFiles();
    if (children == null) {
      return;
    }
    for (File child : children) {
      if (child.isFile() && child.getName().endsWith(INPROGRESS_MARKER_SUFFIX)) {
        String jobId = child.getName().substring(
            0, child.getName().length() - INPROGRESS_MARKER_SUFFIX.length());
        if (isUuidDirectoryName(jobId)) {
          removeIncompleteExportArtifacts(jobId);
        }
      }
    }
    for (File child : children) {
      if (child.isDirectory() && isUuidDirectoryName(child.getName())) {
        removeIncompleteExportArtifacts(child.getName());
      }
    }
  }

  private void removeIncompleteExportArtifacts(String jobId) {
    LOG.info("Removing incomplete container export artifacts for job {}", jobId);
    FileUtils.deleteQuietly(inprogressMarkerFile(jobId));
    File tar = findTarForJobId(jobId);
    if (tar != null) {
      FileUtils.deleteQuietly(tar);
      LOG.info("Removed incomplete container export TAR for job {}: {}", jobId, tar.getName());
    }
    File jobWorkDir = new File(exportDirectory, jobId);
    if (jobWorkDir.isDirectory()) {
      FileUtils.deleteQuietly(jobWorkDir);
      LOG.info("Removed orphaned container export work directory: {}",
          jobWorkDir.getAbsolutePath());
    }
  }

  private File findTarForJobId(String jobId) {
    File exportDir = new File(exportDirectory);
    File[] matches = exportDir.listFiles(
        (dir, name) -> name.endsWith("-" + jobId + ".tar"));
    if (matches == null || matches.length == 0) {
      return null;
    }
    return matches[0];
  }

  private File inprogressMarkerFile(String jobId) {
    return new File(exportDirectory, jobId + INPROGRESS_MARKER_SUFFIX);
  }

  private void markExportInProgress(String jobId) throws IOException {
    Files.createFile(inprogressMarkerFile(jobId).toPath());
  }

  private void clearExportInProgress(String jobId) {
    FileUtils.deleteQuietly(inprogressMarkerFile(jobId));
  }

  private static boolean isUuidDirectoryName(String name) {
    try {
      UUID.fromString(name);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private void executeExport(ExportJob job) {
    Path jobDir = Paths.get(exportDirectory, job.getJobId());
    Path workDir = jobDir.resolve("work");
    File tarFile = new File(job.getTarPath());
    long startTimeMs = System.currentTimeMillis();
    job.setStartTimeMs(startTimeMs);
    boolean succeeded = false;
    Archiver.AppendableTar appendableTar = null;

    try {
      Files.createDirectories(workDir);
      job.setState(ContainerExportStatus.State.RUNNING);
      markExportInProgress(job.getJobId());

      ContainerID cursor = job.getStartContainerId();
      int pageSize = job.getPageSize();
      int shardSize = job.getShardSize();
      int fileIndex = 1;
      long totalRows = 0;
      long recordsInCurrentFile = 0;
      BufferedWriter writer = null;
      Path currentShardPath = null;
      // Pre-allocated buffer: ~12 chars per ID (up to 20 digits + newline) per page.
      StringBuilder buf = new StringBuilder(pageSize * 12);

      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Job cancelled");
          }
          if (!isLeaderReady.getAsBoolean()) {
            throw new IOException("SCM lost leadership during export");
          }

          int fetchCount = pageSize;
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
              currentShardPath = workDir.resolve(shardFileName(job, fileIndex));
              writer = Files.newBufferedWriter(currentShardPath, StandardCharsets.UTF_8);
              writeMetadataHeader(writer, job, fileIndex, containerId);
              LOG.info("Export job {} created shard part{}", job.getJobId(), fileIndex);
            }

            buf.append(containerId.getProtobuf().getId()).append('\n');
            totalRows++;
            recordsInCurrentFile++;
            job.setTotalRows(totalRows);

            if (recordsInCurrentFile >= shardSize) {
              writer.write(buf.toString());
              buf.setLength(0);
              writer = closeWriter(writer);
              if (appendableTar == null) {
                appendableTar = Archiver.openForAppend(tarFile);
              }
              appendShardToTar(appendableTar, currentShardPath, shardEntryName(job, fileIndex));
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
          clearExportInProgress(job.getJobId());
          FileUtils.deleteQuietly(workDir.toFile());
          FileUtils.deleteQuietly(jobDir.toFile());
          job.setState(ContainerExportStatus.State.SUCCEEDED);
          job.setTarPath(null);
          succeeded = true;
          LOG.info("Export job {} completed with zero matching containers", job.getJobId());
          return;
        }

        if (currentShardPath != null) {
          if (appendableTar == null) {
            appendableTar = Archiver.openForAppend(tarFile);
          }
          appendShardToTar(appendableTar, currentShardPath, shardEntryName(job, fileIndex));
        }

        FileUtils.deleteQuietly(workDir.toFile());
        FileUtils.deleteQuietly(jobDir.toFile());
        job.setState(ContainerExportStatus.State.SUCCEEDED);
        succeeded = true;
        LOG.info("Export job {} completed ({} rows, tar={}).",
            job.getJobId(), totalRows, tarFile.getAbsolutePath());
      } finally {
        closeWriter(writer);
        if (appendableTar != null) {
          appendableTar.close();
          if (succeeded) {
            clearExportInProgress(job.getJobId());
          }
        }
      }
    } catch (InterruptedException e) {
      job.setState(ContainerExportStatus.State.FAILED);
      job.setErrorMessage("Job was cancelled");
      cleanupFailedArtifacts(jobDir, tarFile, job.getJobId());
      LOG.info("Export job {} was cancelled", job.getJobId());
      Thread.currentThread().interrupt();
    } catch (IOException | RuntimeException e) {
      job.setState(ContainerExportStatus.State.FAILED);
      job.setErrorMessage(e.getMessage() != null ? e.getMessage() : e.toString());
      cleanupFailedArtifacts(jobDir, tarFile, job.getJobId());
      LOG.error("Export job {} failed", job.getJobId(), e);
    } finally {
      if (metrics != null) {
        if (succeeded) {
          metrics.incrExportJobsSucceeded();
          long bytesWritten = tarFile.isFile() ? tarFile.length() : 0L;
          metrics.recordLastSuccessfulExport(job.getTotalRows(), bytesWritten);
        } else if (job.getState() == ContainerExportStatus.State.FAILED) {
          metrics.incrExportJobsFailed();
        }
      }
      if (job.getState() == ContainerExportStatus.State.SUCCEEDED
          || job.getState() == ContainerExportStatus.State.FAILED) {
        synchronized (submitLock) {
          evictOldTerminalJobs();
        }
      }
    }
  }

  private static String shardFileName(ExportJob job, int partIndex) {
    return String.format("container-ids-%s-%s-part%03d.txt",
        job.getScope(), job.getTimestamp(), partIndex);
  }

  private static String shardEntryName(ExportJob job, int partIndex) {
    return shardFileName(job, partIndex);
  }

  private static void appendShardToTar(Archiver.AppendableTar tar, Path shardPath, String entryName)
      throws IOException {
    tar.appendFile(shardPath.toFile(), entryName);
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

  private static void writeMetadataHeader(BufferedWriter writer, ExportJob job,
      int partNumber, ContainerID shardStartContainerId) throws IOException {
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
    writer.write("# startContainerId=" + shardStartContainerId.getProtobuf().getId());
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
    private final int pageSize;
    private final int shardSize;
    private volatile String tarPath;
    private volatile ContainerExportStatus.State state =
        ContainerExportStatus.State.RUNNING;
    private volatile long totalRows;
    private volatile long startTimeMs;
    private volatile long endTimeMs;
    private volatile String errorMessage;

    @SuppressWarnings("checkstyle:ParameterNumber")
    ExportJob(String jobId, String scope, String timestamp, String tarPath,
        ContainerID startContainerId, LifeCycleState lifeCycleState,
        ContainerHealthState healthState, long maxRows, int pageSize, int shardSize) {
      this.jobId = jobId;
      this.scope = scope;
      this.timestamp = timestamp;
      this.tarPath = tarPath;
      this.startContainerId = startContainerId != null
          ? startContainerId : ContainerID.valueOf(0);
      this.lifeCycleState = lifeCycleState;
      this.healthState = healthState;
      this.maxRows = maxRows;
      this.pageSize = pageSize;
      this.shardSize = shardSize;
    }

    int getPageSize() {
      return pageSize;
    }

    int getShardSize() {
      return shardSize;
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

    long getEndTimeMs() {
      return endTimeMs;
    }

    long getTotalRows() {
      return totalRows;
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
      return new ContainerExportStatus(jobId, state, lifeCycleState, healthState, totalRows,
          elapsed, tarPath, errorMessage);
    }
  }
}
