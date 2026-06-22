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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.utils.Archiver;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ContainerExportManager}.
 */
public class TestContainerExportManager {

  private static final int TEST_PAGE_SIZE = 2;
  private static final int TEST_SHARD_RECORDS = 3;

  @TempDir
  private File tempDir;

  private ContainerManager containerManager;
  private ContainerExportManager exportManager;

  @BeforeEach
  public void setup() {
    containerManager = mock(ContainerManager.class);
    exportManager = new ContainerExportManager(
        containerManager, tempDir.getAbsolutePath(),
        TEST_SHARD_RECORDS, TEST_PAGE_SIZE);
  }

  @AfterEach
  public void teardown() {
    exportManager.shutdown();
  }

  @Test
  public void testMultiShardExportCreatesTar() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(3)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    String jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);

    ContainerExportStatus status = waitForTerminal(jobId);
    if (status.getState() == ContainerExportStatus.State.FAILED) {
      fail(status.getErrorMessage());
    }
    assertEquals(ContainerExportStatus.State.SUCCEEDED, status.getState());
    assertEquals(4, status.getTotalRows());
    assertNotNull(status.getTarPath());
    assertTrue(status.getTarPath().endsWith(".tar"));
    assertTrue(new File(status.getTarPath()).exists());

    Path extractDir = Files.createTempDirectory("export-tar");
    try {
      Archiver.extract(new File(status.getTarPath()), extractDir);
      String part2Name = Arrays.stream(extractDir.toFile().list())
          .filter(name -> name.endsWith("part002.txt"))
          .findFirst()
          .orElseThrow(() -> new AssertionError("part002.txt not found in TAR"));
      assertTrue(Files.readAllLines(extractDir.resolve(part2Name)).contains(
          "# startContainerId=4"));
    } finally {
      FileUtils.deleteQuietly(extractDir.toFile());
    }
  }

  @Test
  public void testSingleShardExportCreatesTar() throws Exception {
    exportManager.shutdown();
    exportManager = new ContainerExportManager(
        containerManager, tempDir.getAbsolutePath(),
        100, TEST_PAGE_SIZE);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1, 2, 3, 4));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(5)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    String jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);

    ContainerExportStatus status = waitForTerminal(jobId);
    if (status.getState() == ContainerExportStatus.State.FAILED) {
      fail(status.getErrorMessage());
    }
    assertEquals(ContainerExportStatus.State.SUCCEEDED, status.getState());
    assertTrue(status.getTarPath().endsWith(".tar"));
    assertTrue(new File(status.getTarPath()).exists());
    assertFalse(new File(status.getTarPath().replace(".tar", ".txt")).exists());
  }

  @Test
  public void testRejectConcurrentExport() {
    exportManager.getJobTracker().put("existing",
        newRunningJob("existing"));
    assertThrows(IllegalStateException.class, () ->
        exportManager.submitJob(ContainerID.valueOf(0), null,
            ContainerHealthState.EMPTY, 0));
  }

  @Test
  public void testRejectMissingFilters() {
    assertThrows(IllegalArgumentException.class, () ->
        exportManager.submitJob(ContainerID.valueOf(0), null, null, 0));
  }

  @Test
  public void testBuildScope() {
    assertEquals("health-MISSING",
        ContainerExportManager.buildScope(null, ContainerHealthState.MISSING));
  }

  @Test
  public void testMetadataTimestampIsHumanReadable() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    String jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);
    waitForTerminal(jobId);

    ContainerExportManager.ExportJob job = exportManager.getJobTracker().get(jobId);
    assertTrue(job.getTimestamp().matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"));
  }

  @Test
  public void testElapsedMsStableAfterCompletion() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    String jobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);
    ContainerExportStatus first = waitForTerminal(jobId);
    Thread.sleep(200);
    ContainerExportStatus second = exportManager.getJobStatus(jobId);
    assertEquals(first.getElapsedMs(), second.getElapsedMs());
  }

  @Test
  public void testRecentTerminalJobRetainedAcrossSubmits() throws Exception {
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(ids(1));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(2)), anyInt(), isNull(), eq(ContainerHealthState.MISSING)))
        .thenReturn(Collections.emptyList());

    String firstJobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);
    waitForTerminal(firstJobId);

    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(0)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(ids(10));
    when(containerManager.getContainerIDs(
        eq(ContainerID.valueOf(11)), anyInt(), isNull(), eq(ContainerHealthState.EMPTY)))
        .thenReturn(Collections.emptyList());

    String secondJobId = exportManager.submitJob(ContainerID.valueOf(0), null,
        ContainerHealthState.EMPTY, 0);
    waitForTerminal(secondJobId);

    assertNotNull(exportManager.getJobStatus(firstJobId));
    assertNotNull(exportManager.getJobStatus(secondJobId));
  }

  private static List<ContainerID> ids(long... values) {
    return Arrays.stream(values).mapToObj(ContainerID::valueOf)
        .collect(Collectors.toList());
  }

  private ContainerExportStatus waitForTerminal(String jobId) throws Exception {
    GenericTestUtils.waitFor(() -> {
      ContainerExportStatus status = exportManager.getJobStatus(jobId);
      return status != null && status.isTerminal();
    }, 100, 30_000);
    return exportManager.getJobStatus(jobId);
  }

  private static ContainerExportManager.ExportJob newRunningJob(String jobId) {
    ContainerExportManager.ExportJob job = new ContainerExportManager.ExportJob(
        jobId, "health-MISSING", "2026-01-01T00:00:00Z",
        "/tmp/test.tar", "/tmp/exports", ContainerID.valueOf(0), null,
        ContainerHealthState.MISSING, 0);
    job.setState(ContainerExportStatus.State.RUNNING);
    return job;
  }
}
