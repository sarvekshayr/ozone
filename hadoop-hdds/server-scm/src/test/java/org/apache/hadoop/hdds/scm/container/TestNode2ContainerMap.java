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

package org.apache.hadoop.hdds.scm.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.ReportResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test classes for Node2ContainerMap.
 */
public class TestNode2ContainerMap {
  private static final int DATANODE_COUNT = 300;
  private static final int CONTAINER_COUNT = 1000;
  private final Map<UUID, TreeSet<ContainerID>> testData = new
      ConcurrentHashMap<>();

  private void generateData() {
    for (int dnIndex = 1; dnIndex <= DATANODE_COUNT; dnIndex++) {
      TreeSet<ContainerID> currentSet = new TreeSet<>();
      for (int cnIndex = 1; cnIndex <= CONTAINER_COUNT; cnIndex++) {
        long currentCnIndex = (long) (dnIndex * CONTAINER_COUNT) + cnIndex;
        currentSet.add(ContainerID.valueOf(currentCnIndex));
      }
      testData.put(UUID.randomUUID(), currentSet);
    }
  }

  private UUID getFirstKey() {
    return testData.keySet().iterator().next();
  }

  @BeforeEach
  public void setUp() throws Exception {
    generateData();
  }

  @Test
  public void testIsKnownDatanode() throws SCMException {
    Node2ContainerMap map = new Node2ContainerMap();
    UUID knownNode = getFirstKey();
    UUID unknownNode = UUID.randomUUID();
    Set<ContainerID> containerIDs = testData.get(knownNode);
    map.insertNewDatanode(knownNode, containerIDs);
    assertTrue(map.isKnownDatanode(knownNode),
        "Not able to detect a known node");
    assertFalse(map.isKnownDatanode(unknownNode),
        "Unknown node detected");
  }

  @Test
  public void testInsertNewDatanode() throws SCMException {
    Node2ContainerMap map = new Node2ContainerMap();
    UUID knownNode = getFirstKey();
    Set<ContainerID> containerIDs = testData.get(knownNode);
    map.insertNewDatanode(knownNode, containerIDs);
    Set<ContainerID> readSet = map.getContainers(knownNode);

    // Assert that all elements are present in the set that we read back from
    // node map.
    Set newSet = new TreeSet((readSet));
    assertTrue(newSet.removeAll(containerIDs));
    assertEquals(0, newSet.size());

    Throwable t = assertThrows(SCMException.class,
        () -> map.insertNewDatanode(knownNode, containerIDs));
    assertEquals("Node already exists in the map", t.getMessage());

    map.removeDatanode(knownNode);
    map.insertNewDatanode(knownNode, containerIDs);

  }

  @Test
  public void testProcessReportCheckOneNode() throws SCMException {
    UUID key = getFirstKey();
    Set<ContainerID> values = testData.get(key);
    Node2ContainerMap map = new Node2ContainerMap();
    map.insertNewDatanode(key, values);
    assertTrue(map.isKnownDatanode(key));
    ReportResult result = map.processReport(key, values);
    assertEquals(ReportResult.ReportStatus.ALL_IS_WELL, result.getStatus());
  }

  @Test
  public void testUpdateDatanodeMap() throws SCMException {
    UUID datanodeId = getFirstKey();
    Set<ContainerID> values = testData.get(datanodeId);
    Node2ContainerMap map = new Node2ContainerMap();
    map.insertNewDatanode(datanodeId, values);
    assertTrue(map.isKnownDatanode(datanodeId));
    assertEquals(CONTAINER_COUNT,
        map.getContainers(datanodeId).size());

    //remove one container
    values.remove(values.iterator().next());
    assertEquals(CONTAINER_COUNT - 1, values.size());
    assertEquals(CONTAINER_COUNT, map.getContainers(datanodeId).size());

    map.setContainersForDatanode(datanodeId, values);

    assertEquals(values.size(), map.getContainers(datanodeId).size());
    assertEquals(values, map.getContainers(datanodeId));
  }

  @Test
  public void testProcessReportInsertAll() throws SCMException {
    Node2ContainerMap map = new Node2ContainerMap();

    for (Map.Entry<UUID, TreeSet<ContainerID>> keyEntry : testData.entrySet()) {
      map.insertNewDatanode(keyEntry.getKey(), keyEntry.getValue());
    }
    // Assert all Keys are known datanodes.
    for (UUID key : testData.keySet()) {
      assertTrue(map.isKnownDatanode(key));
    }
  }

  /*
  For ProcessReport we have to test the following scenarios.

  1. New Datanode - A new datanode appears and we have to add that to the
  SCM's Node2Container Map.

  2.  New Container - A Datanode exists, but a new container is added to that
   DN. We need to detect that and return a list of added containers.

  3. Missing Container - A Datanode exists, but one of the expected container
   on that datanode is missing. We need to detect that.

   4. We get a container report that has both the missing and new containers.
    We need to return separate lists for these.
   */

  /**
   * Assert that we are able to detect the addition of a new datanode.
   *
   * @throws SCMException
   */
  @Test
  public void testProcessReportDetectNewDataNode() {
    Node2ContainerMap map = new Node2ContainerMap();
    // If we attempt to process a node that is not present in the map,
    // we get a result back that says, NEW_NODE_FOUND.
    UUID key = getFirstKey();
    TreeSet<ContainerID> values = testData.get(key);
    ReportResult result = map.processReport(key, values);
    assertEquals(ReportResult.ReportStatus.NEW_DATANODE_FOUND, result.getStatus());
    assertEquals(result.getNewEntries().size(), values.size());
  }

  /**
   * This test asserts that processReport is able to detect new containers
   * when it is added to a datanode. For that we populate the DN with a list
   * of containerIDs and then add few more containers and make sure that we
   * are able to detect them.
   *
   * @throws SCMException
   */
  @Test
  public void testProcessReportDetectNewContainers() throws SCMException {
    Node2ContainerMap map = new Node2ContainerMap();
    UUID key = getFirstKey();
    TreeSet<ContainerID> values = testData.get(key);
    map.insertNewDatanode(key, values);

    final int newCount = 100;
    ContainerID last = values.last();
    TreeSet<ContainerID> addedContainers = new TreeSet<>();
    for (int x = 1; x <= newCount; x++) {
      long cTemp = last.getId() + x;
      addedContainers.add(ContainerID.valueOf(cTemp));
    }

    // This set is the super set of existing containers and new containers.
    TreeSet<ContainerID> newContainersSet = new TreeSet<>(values);
    newContainersSet.addAll(addedContainers);

    ReportResult result = map.processReport(key, newContainersSet);

    //Assert that expected size of missing container is same as addedContainers
    assertEquals(ReportResult.ReportStatus.NEW_ENTRIES_FOUND, result.getStatus());

    assertEquals(addedContainers.size(), result.getNewEntries().size());

    // Assert that the Container IDs are the same as we added new.
    assertTrue(result.getNewEntries().removeAll(addedContainers),
        "All objects are not removed.");
  }

  /**
   * This test asserts that processReport is able to detect missing containers
   * if they are misssing from a list.
   *
   * @throws SCMException
   */
  @Test
  public void testProcessReportDetectMissingContainers() throws SCMException {
    Node2ContainerMap map = new Node2ContainerMap();
    UUID key = getFirstKey();
    TreeSet<ContainerID> values = testData.get(key);
    map.insertNewDatanode(key, values);

    final int removeCount = 100;
    Random r = new Random();

    ContainerID first = values.first();
    TreeSet<ContainerID> removedContainers = new TreeSet<>();

    // Pick a random container to remove it is ok to collide no issues.
    for (int x = 0; x < removeCount; x++) {
      int startBase = (int) first.getId();
      long cTemp = r.nextInt(values.size());
      removedContainers.add(ContainerID.valueOf(cTemp + startBase));
    }

    // This set is a new set with some containers removed.
    TreeSet<ContainerID> newContainersSet = new TreeSet<>(values);
    newContainersSet.removeAll(removedContainers);

    ReportResult result = map.processReport(key, newContainersSet);


    //Assert that expected size of missing container is same as addedContainers
    assertEquals(ReportResult.ReportStatus.MISSING_ENTRIES, result.getStatus());
    assertEquals(removedContainers.size(), result.getMissingEntries().size());

    // Assert that the Container IDs are the same as we added new.
    assertTrue(
        result.getMissingEntries().removeAll(removedContainers),
        "All missing containers not found.");
  }

  @Test
  public void testProcessReportDetectNewAndMissingContainers() throws
      SCMException {
    Node2ContainerMap map = new Node2ContainerMap();
    UUID key = getFirstKey();
    TreeSet<ContainerID> values = testData.get(key);
    map.insertNewDatanode(key, values);

    Set<ContainerID> insertedSet = new TreeSet<>();
    // Insert nodes from 1..30
    for (int x = 1; x <= 30; x++) {
      insertedSet.add(ContainerID.valueOf(x));
    }


    final int removeCount = 100;
    Random r = new Random();

    ContainerID first = values.first();
    TreeSet<ContainerID> removedContainers = new TreeSet<>();

    // Pick a random container to remove it is ok to collide no issues.
    for (int x = 0; x < removeCount; x++) {
      int startBase = (int) first.getId();
      long cTemp = r.nextInt(values.size());
      removedContainers.add(ContainerID.valueOf(cTemp + startBase));
    }

    Set<ContainerID> newSet = new TreeSet<>(values);
    newSet.addAll(insertedSet);
    newSet.removeAll(removedContainers);

    ReportResult result = map.processReport(key, newSet);


    assertEquals(ReportResult.ReportStatus.MISSING_AND_NEW_ENTRIES_FOUND, result.getStatus());
    assertEquals(removedContainers.size(), result.getMissingEntries().size());


    // Assert that the Container IDs are the same as we added new.
    assertTrue(result.getMissingEntries().removeAll(removedContainers), "All missing containers not found.");

    assertEquals(insertedSet.size(), result.getNewEntries().size());

    // Assert that the Container IDs are the same as we added new.
    assertTrue(result.getNewEntries().removeAll(insertedSet), "All inserted containers are not found.");
  }
}
