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

package org.apache.hadoop.hdds.utils;

import static java.util.stream.Collectors.toList;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.archivers.tar.TarUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create and extract archives. */
public final class Archiver {

  static final int MIN_BUFFER_SIZE = 8 * (int) OzoneConsts.KB; // same as IOUtils.DEFAULT_BUFFER_SIZE
  static final int MAX_BUFFER_SIZE = (int) OzoneConsts.MB;
  private static final int TAR_SIZE_OFFSET = 124;
  private static final int TAR_SIZE_LENGTH = 12;
  private static final Logger LOG = LoggerFactory.getLogger(Archiver.class);

  private Archiver() {
    // no instances (for now)
  }

  /** Create tarball including contents of {@code from}. */
  public static void create(File tarFile, Path from) throws IOException {
    try (ArchiveOutputStream<TarArchiveEntry> out = tar(Files.newOutputStream(tarFile.toPath()))) {
      includePath(from, "", out);
    }
  }

  /**
   * Append a single file as a new entry to an existing tarball, or create the
   * tarball if it does not exist yet.
   */
  public static void appendFile(File tarFile, File file, String entryName)
      throws IOException {
    if (tarFile.exists() && tarFile.length() > 0) {
      stripTarEofMarker(tarFile);
    }
    OpenOption[] options = tarFile.exists() && tarFile.length() > 0
        ? new OpenOption[] {StandardOpenOption.WRITE, StandardOpenOption.APPEND}
        : new OpenOption[] {StandardOpenOption.WRITE, StandardOpenOption.CREATE};
    try (OutputStream fos = Files.newOutputStream(tarFile.toPath(), options);
         ArchiveOutputStream<TarArchiveEntry> out = simpleTar(fos)) {
      includeSimpleFile(file, entryName, out);
      out.finish();
    }
  }

  /**
   * Remove the tar end-of-archive marker so new entries can be appended.
   */
  private static void stripTarEofMarker(File tarFile) throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(tarFile, "rw")) {
      long position = 0;
      long fileLength = raf.length();
      byte[] header = new byte[TarConstants.DEFAULT_RCDSIZE];
      while (position + TarConstants.DEFAULT_RCDSIZE <= fileLength) {
        raf.seek(position);
        raf.readFully(header);
        if (isZeroBlock(header)) {
          raf.setLength(position);
          return;
        }
        long entrySize = parseTarEntrySize(header);
        position += TarConstants.DEFAULT_RCDSIZE + paddedTarEntrySize(entrySize);
      }
      throw new IOException("Invalid tar archive without an end-of-archive marker: " + tarFile);
    }
  }

  private static boolean isZeroBlock(byte[] block) {
    for (byte b : block) {
      if (b != 0) {
        return false;
      }
    }
    return true;
  }

  private static long parseTarEntrySize(byte[] header) throws IOException {
    try {
      return TarUtils.parseOctalOrBinary(header, TAR_SIZE_OFFSET, TAR_SIZE_LENGTH);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid tar entry size.", e);
    }
  }

  private static long paddedTarEntrySize(long size) {
    long recordSize = TarConstants.DEFAULT_RCDSIZE;
    return ((size + recordSize - 1) / recordSize) * recordSize;
  }

  /** Extract {@code tarFile} to {@code dir}. */
  public static void extract(File tarFile, Path dir) throws IOException {
    Files.createDirectories(dir);
    String parent = dir.toString();
    try (ArchiveInputStream<TarArchiveEntry> in = untar(Files.newInputStream(tarFile.toPath()))) {
      TarArchiveEntry entry;
      while ((entry = in.getNextEntry()) != null) {
        Path path = Paths.get(parent, entry.getName());
        extractEntry(entry, in, entry.getSize(), dir, path);
      }
    }
  }

  public static byte[] readEntry(InputStream input, final long size)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    IOUtils.copy(input, output, getBufferSize(size));
    return output.toByteArray();
  }

  private static TarArchiveEntry createSimpleTarArchiveEntry(File file, String entryName)
      throws IOException {
    TarArchiveEntry entry = new TarArchiveEntry(entryName);
    entry.setMode(TarArchiveEntry.DEFAULT_FILE_MODE);
    entry.setSize(Files.size(file.toPath()));
    entry.setModTime(file.lastModified());
    return entry;
  }

  private static long includeSimpleFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {
    TarArchiveEntry entry = createSimpleTarArchiveEntry(file, entryName);
    archiveOutput.putArchiveEntry(entry);
    try (InputStream input = Files.newInputStream(file.toPath())) {
      return IOUtils.copy(input, archiveOutput, getBufferSize(file.length()));
    } finally {
      archiveOutput.closeArchiveEntry();
    }
  }

  private static ArchiveOutputStream<TarArchiveEntry> simpleTar(OutputStream output) {
    TarArchiveOutputStream os = new TarArchiveOutputStream(output);
    os.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    return os;
  }

  private static TarArchiveEntry createBasicTarArchiveEntry(File file, String entryName)
      throws IOException {
    final Path path = file.toPath();

    final TarArchiveEntry entry;
    if (Files.isDirectory(path)) {
      final int nameLength = entryName.length();
      final String dirName = nameLength == 0 || entryName.charAt(nameLength - 1) != '/'
          ? entryName + "/"
          : entryName;
      entry = new TarArchiveEntry(dirName, TarConstants.LF_DIR);
      entry.setMode(TarArchiveEntry.DEFAULT_DIR_MODE);
    } else {
      entry = new TarArchiveEntry(entryName);
      entry.setMode(TarArchiveEntry.DEFAULT_FILE_MODE);
      entry.setSize(Files.size(path));
    }

    try {
      BasicFileAttributes attrs = Files.readAttributes(
          file.toPath(), BasicFileAttributes.class);
      entry.setLastModifiedTime(attrs.lastModifiedTime());
      entry.setLastAccessTime(attrs.lastAccessTime());
      entry.setCreationTime(attrs.creationTime());
    } catch (IOException e) {
      entry.setModTime(file.lastModified()); // fallback
    }
    return entry;
  }

  public static void includePath(Path dir, String subdir,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {

    // Add a directory entry before adding files, in case the directory is
    // empty.
    TarArchiveEntry entry = createBasicTarArchiveEntry(dir.toFile(), subdir);
    archiveOutput.putArchiveEntry(entry);
    archiveOutput.closeArchiveEntry();

    // Add files in the directory.
    try (Stream<Path> dirEntries = Files.list(dir)) {
      for (Path path : dirEntries.collect(toList())) {
        File file = path.toFile();
        String entryName = subdir + "/" + path.getFileName();
        if (file.isDirectory()) {
          includePath(path, entryName, archiveOutput);
        } else {
          includeFile(file, entryName, archiveOutput);
        }
      }
    }
  }

  public static long includeFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput) throws IOException {
    final long bytes;
    TarArchiveEntry entry = createBasicTarArchiveEntry(file, entryName);
    archiveOutput.putArchiveEntry(entry);
    try (InputStream input = Files.newInputStream(file.toPath())) {
      bytes = IOUtils.copy(input, archiveOutput, getBufferSize(file.length()));
    }
    archiveOutput.closeArchiveEntry();
    return bytes;
  }

  /**
   * Creates a hard link to the specified file in the provided temporary directory,
   * adds the linked file as an entry to the archive with the given entry name, writes
   * its contents to the archive output, and then deletes the temporary hard link.
   * <p>
   * This approach avoids altering the original file and works around limitations
   * of certain archiving libraries that may require the source file to be present
   * in a specific location or have a specific name. Any errors during the hardlink
   * creation or archiving process are logged.
   * </p>
   *
   * @param file         the file to be included in the archive
   * @param entryName    the name/path under which the file should appear in the archive
   * @param archiveOutput the output stream for the archive (e.g., tar)
   * @param tmpDir       the temporary directory in which to create the hard link
   * @return number of bytes copied to the archive for this file
   * @throws IOException if an I/O error occurs other than hardlink creation failure
   */
  public static long linkAndIncludeFile(File file, String entryName,
      ArchiveOutputStream<TarArchiveEntry> archiveOutput, Path tmpDir) throws IOException {
    File link = tmpDir.resolve(entryName).toFile();
    long bytes = 0;
    try {
      Files.createLink(link.toPath(), file.toPath());
      TarArchiveEntry entry = createBasicTarArchiveEntry(link, entryName);
      archiveOutput.putArchiveEntry(entry);
      try (InputStream input = Files.newInputStream(link.toPath())) {
        bytes = IOUtils.copyLarge(input, archiveOutput);
      }
      archiveOutput.closeArchiveEntry();
    } catch (IOException ioe) {
      LOG.error("Couldn't create hardlink for file {} while including it in tarball.",
          file.getAbsolutePath(), ioe);
      throw ioe;
    } finally {
      Files.deleteIfExists(link.toPath());
    }
    return bytes;
  }

  public static void extractEntry(ArchiveEntry entry, InputStream input, long size,
      Path ancestor, Path path) throws IOException {
    HddsUtils.validatePath(path, ancestor);

    if (entry.isDirectory()) {
      Files.createDirectories(path);
    } else {
      Path parent = path.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      try (OutputStream fileOutput = Files.newOutputStream(path);
           OutputStream output = new BufferedOutputStream(fileOutput)) {
        IOUtils.copy(input, output, getBufferSize(size));
      }
    }
  }

  public static ArchiveInputStream<TarArchiveEntry> untar(InputStream input) {
    return new TarArchiveInputStream(input);
  }

  public static ArchiveOutputStream<TarArchiveEntry> tar(OutputStream output) {
    TarArchiveOutputStream os = new TarArchiveOutputStream(output);
    os.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
    os.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
    return os;
  }

  static int getBufferSize(long fileSize) {
    return Math.toIntExact(Math.min(MAX_BUFFER_SIZE, Math.max(fileSize, MIN_BUFFER_SIZE)));
  }

}
