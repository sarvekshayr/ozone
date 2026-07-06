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

/**
 * Client-side view of a container ID export job on SCM.
 */
public final class ContainerExportStatus {

  private final String jobId;
  private final State state;
  private final String lifecycleState;
  private final String healthState;
  private final long totalRows;
  private final long elapsedMs;
  private final String tarPath;
  private final String errorMessage;

  /**
   * States of export job.
   */
  public enum State {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerExportStatus(String jobId, State state, String lifecycleState, String healthState,
      long totalRows, long elapsedMs, String tarPath, String errorMessage) {
    this.jobId = jobId;
    this.state = state;
    this.lifecycleState = lifecycleState;
    this.healthState = healthState;
    this.totalRows = totalRows;
    this.elapsedMs = elapsedMs;
    this.tarPath = tarPath;
    this.errorMessage = errorMessage;
  }

  public String getJobId() {
    return jobId;
  }

  public State getState() {
    return state;
  }

  public String getLifecycleState() {
    return lifecycleState;
  }

  public String getHealthState() {
    return healthState;
  }

  public long getTotalRows() {
    return totalRows;
  }

  public long getElapsedMs() {
    return elapsedMs;
  }

  public String getTarPath() {
    return tarPath;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isTerminal() {
    return state == State.SUCCEEDED || state == State.FAILED;
  }
}
