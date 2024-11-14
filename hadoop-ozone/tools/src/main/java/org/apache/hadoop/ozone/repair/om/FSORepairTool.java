/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.repair.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RocksDatabase;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.TableConfig;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.managed.ManagedWriteOptions;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.ratis.util.Preconditions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Base Tool to identify disconnected FSO trees in all buckets.
 * The tool will log information about unreachable files or directories.
 * If deletes are still in progress (the deleted directory table is not empty), the tool may
 * report that the tree is disconnected, even though pending deletes would
 * fix the issue.
 *
 * Before using the tool, make sure all OMs are stopped,
 * and that all Ratis logs have been flushed to the OM DB. This can be
 * done using `ozone admin prepare` before running the tool, and `ozone admin
 * cancelprepare` when done.
 *
 * The tool will run a DFS from each bucket, and save all reachable
 * directories as keys in a new temporary RocksDB instance called "reachable.db"
 * In the same directory as om.db.
 * will then scan the entire file and directory tables for each bucket to see
 * if each object's parent is in the reachable table of reachable.db. The
 * reachable table will be dropped and recreated for each bucket.
 * The tool is idempotent. reachable.db will not be deleted automatically
 * when the tool finishes, in case users want to manually inspect it. It can
 * be safely deleted once the tool finishes.
 */
public class FSORepairTool {
  public static final Logger LOG =
      LoggerFactory.getLogger(org.apache.hadoop.ozone.repair.om.FSORepairTool.class);

  private final String omDBPath;
  private final DBStore store;
  private final Table<String, OmVolumeArgs> volumeTable;
  private final Table<String, OmBucketInfo> bucketTable;
  private final Table<String, OmDirectoryInfo> directoryTable;
  private final Table<String, OmKeyInfo> fileTable;
  private final Table<String, OmKeyInfo> deletedDirectoryTable;
  private final Table<String, RepeatedOmKeyInfo> deletedTable;
  private final Table<String, SnapshotInfo> snapshotInfoTable;
  private final String volumeFilter;
  private final String bucketFilter;
  // The temporary DB is used to track which items have been seen.
  // Since usage of this DB is simple, use it directly from
  // RocksDB.
  private String reachableDBPath;
  private static final String REACHABLE_TABLE = "reachable";
  private static final byte[] REACHABLE_TABLE_BYTES =
      REACHABLE_TABLE.getBytes(StandardCharsets.UTF_8);
  private ColumnFamilyHandle reachableCFHandle;
  private RocksDatabase reachableDB;

  private final ReportStatistics reachableStats;
  private final ReportStatistics unreachableStats;
  private final boolean repair;

  public FSORepairTool(String dbPath, boolean repair, String volume, String bucket) throws IOException {
    this(getStoreFromPath(dbPath), dbPath, repair, volume, bucket);
  }

  /**
   * Allows passing RocksDB instance from a MiniOzoneCluster directly to this
   * class for testing.
   */
  @VisibleForTesting
  public FSORepairTool(DBStore dbStore, String dbPath, boolean repair, String volume, String bucket)
      throws IOException {
    this.reachableStats = new ReportStatistics(0, 0, 0);
    this.unreachableStats = new ReportStatistics(0, 0, 0);

    this.store = dbStore;
    this.omDBPath = dbPath;
    this.repair = repair;
    this.volumeFilter = volume;
    this.bucketFilter = bucket;
    volumeTable = store.getTable(OmMetadataManagerImpl.VOLUME_TABLE,
        String.class,
        OmVolumeArgs.class);
    bucketTable = store.getTable(OmMetadataManagerImpl.BUCKET_TABLE,
        String.class,
        OmBucketInfo.class);
    directoryTable = store.getTable(OmMetadataManagerImpl.DIRECTORY_TABLE,
        String.class,
        OmDirectoryInfo.class);
    fileTable = store.getTable(OmMetadataManagerImpl.FILE_TABLE,
        String.class,
        OmKeyInfo.class);
    deletedDirectoryTable = store.getTable(
        OmMetadataManagerImpl.DELETED_DIR_TABLE,
        String.class,
        OmKeyInfo.class);
    deletedTable = store.getTable(
        OmMetadataManagerImpl.DELETED_TABLE,
        String.class,
        RepeatedOmKeyInfo.class);
    snapshotInfoTable = store.getTable(
        OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE,
        String.class,
        SnapshotInfo.class);
  }

  protected static DBStore getStoreFromPath(String dbPath) throws IOException {
    File omDBFile = new File(dbPath);
    if (!omDBFile.exists() || !omDBFile.isDirectory()) {
      throw new IOException(String.format("Specified OM DB instance %s does " +
          "not exist or is not a RocksDB directory.", dbPath));
    }
    // Load RocksDB and tables needed.
    return OmMetadataManagerImpl.loadDB(new OzoneConfiguration(),
        new File(dbPath).getParentFile());
  }

  public org.apache.hadoop.ozone.repair.om.FSORepairTool.Report run() throws IOException {

    if (bucketFilter != null && volumeFilter == null) {
      System.out.println("--bucket flag cannot be used without specifying --volume.");
      return null;
    }

    if (volumeFilter != null) {
      OmVolumeArgs volumeArgs = volumeTable.getIfExist(volumeFilter);
      if (volumeArgs == null) {
        System.out.println("Volume '" + volumeFilter + "' does not exist.");
        return null;
      }
    }

    // Iterate all volumes or a specific volume if specified
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             volumeIterator = volumeTable.iterator()) {
      openReachableDB();

      while (volumeIterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> volumeEntry =
            volumeIterator.next();
        String volumeKey = volumeEntry.getKey();

        if (volumeFilter != null && !volumeFilter.equals(volumeKey)) {
          continue;
        }

        System.out.println("Processing volume: " + volumeKey);

        if (bucketFilter != null) {
          OmBucketInfo bucketInfo = bucketTable.getIfExist(volumeKey + "/" + bucketFilter);
          if (bucketInfo == null) {
            //Bucket does not exist in the volume
            System.out.println("Bucket '" + bucketFilter + "' does not exist in volume '" + volumeKey + "'.");
            return null;
          }

          if (bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
            System.out.println("Skipping non-FSO bucket " + bucketFilter);
            continue;
          }

          // Check for snapshots in the specified bucket
          if (checkIfSnapshotExistsForBucket(volumeFilter, bucketFilter)) {
            if (!repair) {
              System.out.println("Snapshot detected in bucket '" + bucketFilter + "'");
            } else {
              System.out.println("Snapshot exists in bucket '" + bucketFilter + "'. " +
                  "Repair is not allowed if snapshots exist.");
              return null;
            }
          }

          processBucket(volumeEntry.getValue(), bucketInfo);
        } else {

        // Iterate all buckets in the volume.
          try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
                   bucketIterator = bucketTable.iterator()) {
            bucketIterator.seek(volumeKey);
            while (bucketIterator.hasNext()) {
              Table.KeyValue<String, OmBucketInfo> bucketEntry =
                  bucketIterator.next();
              String bucketKey = bucketEntry.getKey();
              OmBucketInfo bucketInfo = bucketEntry.getValue();

              if (bucketInfo.getBucketLayout() != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
                System.out.println("Skipping non-FSO bucket " + bucketKey);
                continue;
              }

              // Stop this loop once we have seen all buckets in the current
              // volume.
              if (!bucketKey.startsWith(volumeKey)) {
                break;
              }

              processBucket(volumeEntry.getValue(), bucketInfo);
            }
          }
        }
      }
    } finally {
      closeReachableDB();
    }

    return buildReportAndLog();
  }

  private boolean checkIfSnapshotExistsForBucket(String volumeName, String bucketName) throws IOException {
    if (snapshotInfoTable == null) {
      return false;
    }

    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>> iterator =
            snapshotInfoTable.iterator()) {
      while (iterator.hasNext()) {
        SnapshotInfo snapshotInfo = iterator.next().getValue();
        String snapshotPath = (volumeName + "/" + bucketName).replaceFirst("^/", "");
        if (snapshotInfo.getSnapshotPath().equals(snapshotPath)) {
          return true;
        }
      }
    }
    return false;
  }

  private void processBucket(OmVolumeArgs volume, OmBucketInfo bucketInfo) throws IOException {
    System.out.println("Processing bucket: " + volume.getVolume() + "/" + bucketInfo.getBucketName());
    if (checkIfSnapshotExistsForBucket(volume.getVolume(), bucketInfo.getBucketName())) {
      if (!repair) {
        System.out.println(
            "Snapshot detected in bucket '" + volume.getVolume() + "/" + bucketInfo.getBucketName() + "'. ");
      } else {
        System.out.println(
            "Skipping repair for bucket '" + volume.getVolume() + "/" + bucketInfo.getBucketName() + "' " +
            "due to snapshot presence.");
        return;
      }
    }
    dropReachableTableIfExists();
    createReachableTable();
    markReachableObjectsInBucket(volume, bucketInfo);
    handleUnreachableObjects(volume, bucketInfo);
    dropReachableTableIfExists();
  }

  private Report buildReportAndLog() {
    Report report = new Report.Builder()
        .setReachable(reachableStats)
        .setUnreachable(unreachableStats)
        .build();

    System.out.println("\n" + report);
    return report;
  }

  private void markReachableObjectsInBucket(OmVolumeArgs volume,
                                            OmBucketInfo bucket) throws IOException {
    // Only put directories in the stack.
    // Directory keys should have the form /volumeID/bucketID/parentID/name.
    Stack<String> dirKeyStack = new Stack<>();

    // Since the tool uses parent directories to check for reachability, add
    // a reachable entry for the bucket as well.
    addReachableEntry(volume, bucket, bucket);
    // Initialize the stack with all immediate child directories of the
    // bucket, and mark them all as reachable.
    Collection<String> childDirs =
        getChildDirectoriesAndMarkAsReachable(volume, bucket, bucket);
    dirKeyStack.addAll(childDirs);

    while (!dirKeyStack.isEmpty()) {
      // Get one directory and process its immediate children.
      String currentDirKey = dirKeyStack.pop();
      OmDirectoryInfo currentDir = directoryTable.get(currentDirKey);
      if (currentDir == null) {
        System.out.println("Directory key" + currentDirKey + "to be processed was not found in the " +
              "directory table.");
        continue;
      }

      // TODO revisit this for a more memory efficient implementation,
      //  possibly making better use of RocksDB iterators.
      childDirs = getChildDirectoriesAndMarkAsReachable(volume, bucket,
          currentDir);
      dirKeyStack.addAll(childDirs);
    }
  }

  private void handleUnreachableObjects(OmVolumeArgs volume, OmBucketInfo bucket) throws IOException {
    // Check for unreachable directories in the bucket.
    String bucketPrefix = OM_KEY_PREFIX +
        volume.getObjectID() +
        OM_KEY_PREFIX +
        bucket.getObjectID();

    try (TableIterator<String, ? extends
        Table.KeyValue<String, OmDirectoryInfo>> dirIterator =
             directoryTable.iterator()) {
      dirIterator.seek(bucketPrefix);
      while (dirIterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> dirEntry = dirIterator.next();
        String dirKey = dirEntry.getKey();

        // Only search directories in this bucket.
        if (!dirKey.startsWith(bucketPrefix)) {
          break;
        }

        if (!isReachable(dirKey)) {
          System.out.println("Found unreachable directory: " + dirKey);
          unreachableStats.add(new ReportStatistics(1, 0, 0));

          if (!repair) {
            System.out.println("Marking unreachable directory " + dirKey + " for deletion.");
          } else {
            System.out.println("Deleting unreachable directory " + dirKey);
            OmDirectoryInfo dirInfo = dirEntry.getValue();
            markDirectoryForDeletion(volume.getVolume(), bucket.getBucketName(),
                dirKey, dirInfo);
          }
        }
      }
    }

    // Check for unreachable files
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
             fileIterator = fileTable.iterator()) {
      fileIterator.seek(bucketPrefix);
      while (fileIterator.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> fileEntry = fileIterator.next();
        String fileKey = fileEntry.getKey();
        // Only search files in this bucket.
        if (!fileKey.startsWith(bucketPrefix)) {
          break;
        }

        OmKeyInfo fileInfo = fileEntry.getValue();
        if (!isReachable(fileKey)) {
          System.out.println("Found unreachable file: " + fileKey);
          unreachableStats.add(new ReportStatistics(0, 1, fileInfo.getDataSize()));

          if (!repair) {
            System.out.println("Marking unreachable file " + fileKey + " for deletion." + fileKey);
          } else {
            System.out.println("Deleting unreachable file " + fileKey);
            markFileForDeletion(fileKey, fileInfo);
          }
        } else {
          // NOTE: We are deserializing the proto of every reachable file
          // just to log it's size. If we don't need this information we could
          // save time by skipping this step.
          reachableStats.add(new ReportStatistics(0, 1, fileInfo.getDataSize()));
        }
      }
    }
  }

  protected void markFileForDeletion(String fileKey, OmKeyInfo fileInfo) throws IOException {
    try (BatchOperation batch = store.initBatchOperation()) {
      fileTable.deleteWithBatch(batch, fileKey);

      RepeatedOmKeyInfo originalRepeatedKeyInfo = deletedTable.get(fileKey);
      RepeatedOmKeyInfo updatedRepeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
          fileInfo, fileInfo.getUpdateID(), true);
      // NOTE: The FSO code seems to write the open key entry with the whole
      // path, using the object's names instead of their ID. This would only
      // be possible when the file is deleted explicitly, and not part of a
      // directory delete. It is also not possible here if the file's parent
      // is gone. The name of the key does not matter so just use IDs.
      deletedTable.putWithBatch(batch, fileKey, updatedRepeatedOmKeyInfo);

      System.out.println("Added entry " + fileKey + " to open key table: " + updatedRepeatedOmKeyInfo);
      store.commitBatchOperation(batch);
    }
  }

  protected void markDirectoryForDeletion(String volumeName, String bucketName,
                                        String dirKeyName, OmDirectoryInfo dirInfo) throws IOException {
    try (BatchOperation batch = store.initBatchOperation()) {
      directoryTable.deleteWithBatch(batch, dirKeyName);
      // HDDS-7592: Make directory entries in deleted dir table unique.
      String deleteDirKeyName =
          dirKeyName + OM_KEY_PREFIX + dirInfo.getObjectID();

      // Convert the directory to OmKeyInfo for deletion.
      OmKeyInfo dirAsKeyInfo = OMFileRequest.getOmKeyInfo(
          volumeName, bucketName, dirInfo, dirInfo.getName());
      deletedDirectoryTable.putWithBatch(batch, deleteDirKeyName, dirAsKeyInfo);

      store.commitBatchOperation(batch);
    }
  }

  private Collection<String> getChildDirectoriesAndMarkAsReachable(OmVolumeArgs volume,
                                                                   OmBucketInfo bucket,
                                                                   WithObjectID currentDir) throws IOException {

    Collection<String> childDirs = new ArrayList<>();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmDirectoryInfo>>
             dirIterator = directoryTable.iterator()) {
      String dirPrefix = buildReachableKey(volume, bucket, currentDir);
      // Start searching the directory table at the current directory's
      // prefix to get its immediate children.
      dirIterator.seek(dirPrefix);
      while (dirIterator.hasNext()) {
        Table.KeyValue<String, OmDirectoryInfo> childDirEntry =
            dirIterator.next();
        String childDirKey = childDirEntry.getKey();
        // Stop processing once we have seen all immediate children of this
        // directory.
        if (!childDirKey.startsWith(dirPrefix)) {
          break;
        }
        // This directory was reached by search.
        addReachableEntry(volume, bucket, childDirEntry.getValue());
        childDirs.add(childDirKey);
        reachableStats.add(new ReportStatistics(1, 0, 0));
      }
    }

    return childDirs;
  }

  /**
   * Add the specified object to the reachable table, indicating it is part
   * of the connected FSO tree.
   */
  private void addReachableEntry(OmVolumeArgs volume,
                                 OmBucketInfo bucket, WithObjectID object) throws IOException {
    byte[] reachableKey = buildReachableKey(volume, bucket, object)
        .getBytes(StandardCharsets.UTF_8);
    // No value is needed for this table.
    reachableDB.put(reachableCFHandle, reachableKey, new byte[]{});
  }

  /**
   * Build an entry in the reachable table for the current object, which
   * could be a bucket, file or directory.
   */
  private static String buildReachableKey(OmVolumeArgs volume,
                                          OmBucketInfo bucket, WithObjectID object) {
    return OM_KEY_PREFIX +
        volume.getObjectID() +
        OM_KEY_PREFIX +
        bucket.getObjectID() +
        OM_KEY_PREFIX +
        object.getObjectID();
  }

  /**
   *
   * @param fileOrDirKey The key of a file or directory in RocksDB.
   * @return true if the entry's parent is in the reachable table.
   */
  protected boolean isReachable(String fileOrDirKey) throws IOException {
    byte[] reachableParentKey =
        buildReachableParentKey(fileOrDirKey).getBytes(StandardCharsets.UTF_8);

    return reachableDB.get(reachableCFHandle, reachableParentKey, REACHABLE_TABLE) != null;
  }

  /**
   * Build an entry in the reachable table for the current object's parent
   * object. The object could be a file or directory.
   */
  private static String buildReachableParentKey(String fileOrDirKey) {
    String[] keyParts = fileOrDirKey.split(OM_KEY_PREFIX);
    // Should be /volID/bucketID/parentID/name
    // The first part will be blank since key begins with a slash.
    Preconditions.assertTrue(keyParts.length >= 4);
    String volumeID = keyParts[1];
    String bucketID = keyParts[2];
    String parentID = keyParts[3];

    return OM_KEY_PREFIX +
        volumeID +
        OM_KEY_PREFIX +
        bucketID +
        OM_KEY_PREFIX +
        parentID;
  }

  private void openReachableDB() throws IOException {
    File reachableDBFile = new File(new File(omDBPath).getParentFile(),
        "reachable.db");
    System.out.println("Creating database of reachable directories at " + reachableDBFile);
    // Delete the DB from the last run if it exists.
    if (reachableDBFile.exists()) {
      FileUtils.deleteDirectory(reachableDBFile);
    }
    reachableDBPath = reachableDBFile.toString();
    reachableDB = buildReachableRocksDB(reachableDBFile);
  }

  private RocksDatabase buildReachableRocksDB(File reachableDBFile) throws IOException {
    DBProfile profile = new OzoneConfiguration().getEnum(HDDS_DB_PROFILE, HDDS_DEFAULT_DB_PROFILE);
    Set<TableConfig> tableConfigs = new HashSet<>();

    try {
      tableConfigs.add(new TableConfig("default", profile.getColumnFamilyOptions()));

      return RocksDatabase.open(reachableDBFile,
          profile.getDBOptions(),
          new ManagedWriteOptions(),
          tableConfigs, false);
    } finally {
      for (TableConfig config : tableConfigs) {
        config.close();
      }
    }
  }

  private void closeReachableDB() {
    if (reachableDB != null) {
      reachableDB.close();
    }
  }

  private void dropReachableTableIfExists() throws IOException {
    try {
      List<byte[]>
          availableCFs = reachableDB.listColumnFamiliesEmptyOptions(reachableDBPath);
      boolean cfFound = false;
      for (byte[] cfNameBytes: availableCFs) {
        if (new String(cfNameBytes, UTF_8).equals(new String(REACHABLE_TABLE_BYTES, UTF_8))) {
          cfFound = true;
          break;
        }
      }

      if (cfFound) {
        reachableDB.dropColumnFamily(reachableCFHandle);
      }
    } catch (RocksDBException ex) {
      throw new IOException(ex.getMessage(), ex);
    } finally {
      if (reachableCFHandle != null) {
        reachableCFHandle.close();
      }
    }
  }

  private void createReachableTable() throws IOException {
    reachableCFHandle = reachableDB.createColumnFamily(
        new ColumnFamilyDescriptor(REACHABLE_TABLE_BYTES));
  }

  /**
   * Define a Report to be created.
   */
  public static class Report {
    private final ReportStatistics reachable;
    private final ReportStatistics unreachable;

    /**
     * Builds one report that is the aggregate of multiple others.
     */
    public Report(org.apache.hadoop.ozone.repair.om.FSORepairTool.Report... reports) {
      reachable = new ReportStatistics();
      unreachable = new ReportStatistics();

      for (org.apache.hadoop.ozone.repair.om.FSORepairTool.Report report : reports) {
        reachable.add(report.reachable);
        unreachable.add(report.unreachable);
      }
    }

    private Report(org.apache.hadoop.ozone.repair.om.FSORepairTool.Report.Builder builder) {
      this.reachable = builder.reachable;
      this.unreachable = builder.unreachable;
    }

    public ReportStatistics getReachable() {
      return reachable;
    }

    public ReportStatistics getUnreachable() {
      return unreachable;
    }

    public String toString() {
      return "Reachable: " + reachable + "\nUnreachable: " + unreachable;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      FSORepairTool.Report report = (FSORepairTool.Report) other;

      // Useful for testing.
      System.out.println("Comparing reports\nExpect:\n" + this + "\nActual:\n" + report);

      return reachable.equals(report.reachable) && unreachable.equals(report.unreachable);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reachable, unreachable);
    }

    /**
     * Builder class for a Report.
     */
    public static final class Builder {
      private ReportStatistics reachable = new ReportStatistics();
      private ReportStatistics unreachable = new ReportStatistics();

      public Builder() {
      }

      public Builder setReachable(ReportStatistics reachable) {
        this.reachable = reachable;
        return this;
      }

      public Builder setUnreachable(ReportStatistics unreachable) {
        this.unreachable = unreachable;
        return this;
      }

      public Report build() {
        return new Report(this);
      }
    }
  }

  /**
   * Represents the statistics of reachable and unreachable data.
   * This gives the count of dirs, files and bytes.
   */

  public static class ReportStatistics {
    private long dirs;
    private long files;
    private long bytes;

    public ReportStatistics() { }

    public ReportStatistics(long dirs, long files, long bytes) {
      this.dirs = dirs;
      this.files = files;
      this.bytes = bytes;
    }

    public void add(ReportStatistics other) {
      this.dirs += other.dirs;
      this.files += other.files;
      this.bytes += other.bytes;
    }

    public long getDirs() {
      return dirs;
    }

    public long getFiles() {
      return files;
    }

    public long getBytes() {
      return bytes;
    }

    @Override
    public String toString() {
      return "\n\tDirectories: " + dirs +
              "\n\tFiles: " + files +
              "\n\tBytes: " + bytes;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      ReportStatistics stats = (ReportStatistics) other;

      return bytes == stats.bytes && files == stats.files && dirs == stats.dirs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(bytes, files, dirs);
    }
  }
}
