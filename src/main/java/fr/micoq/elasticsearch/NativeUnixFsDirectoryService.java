/*
 * Copyright 2018 Michaël Coquard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.micoq.elasticsearch;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.HashSet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.store.IndexStore;

import fr.micoq.elasticsearch.NativeUnixDirectory.ForceIO;

public class NativeUnixFsDirectoryService extends FsDirectoryService {

  public NativeUnixFsDirectoryService(IndexSettings indexSettings, IndexStore indexStore, ShardPath path) {
    super(indexSettings, indexStore, path);
  }

  @Override
  protected Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
    boolean mmapEnabled = indexSettings.getValue(NativeUnixStorePlugin.SETTING_MMAP_ENABLED);
    boolean mmapReadAhead = indexSettings.getValue(NativeUnixStorePlugin.SETTING_MMAP_READ_AHEAD);
    boolean directReadEnabled = indexSettings.getValue(NativeUnixStorePlugin.SETTING_DIRECT_READ_ENABLED);
    boolean directWriteEnabled = indexSettings.getValue(NativeUnixStorePlugin.SETTING_DIRECT_WRITE_ENABLED);
    int directReadBufferSize = (int)Math.min(
        indexSettings.getValue(NativeUnixStorePlugin.SETTING_DIRECT_READ_BUFFER_SIZE).getBytes(),
        (long)Integer.MAX_VALUE);
    int directWriteBufferSize = (int)Math.min(
        indexSettings.getValue(NativeUnixStorePlugin.SETTING_DIRECT_WRITE_BUFFER_SIZE).getBytes(),
        (long)Integer.MAX_VALUE);
    long minBytesDirect = indexSettings.getValue(NativeUnixStorePlugin.SETTING_DIRECT_MIN_MERGE_SIZE).getBytes();
    long maxBytesPreload = indexSettings.getValue(NativeUnixStorePlugin.SETTING_MMAP_MAX_PRELOAD_SIZE).getBytes();
    Set<String> preLoadExtensions = new HashSet<>(
        indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
    if(preLoadExtensions.contains("*")) {
      preLoadExtensions = null; // preload all files
    }
    
    return new NativeUnixDirectory(
      location,
      lockFactory,
      mmapEnabled,
      mmapReadAhead,
      directReadEnabled,
      directWriteEnabled,
      directReadBufferSize,
      directWriteBufferSize,
      ForceIO.None,
      minBytesDirect,
      maxBytesPreload,
      preLoadExtensions);
  }
}
