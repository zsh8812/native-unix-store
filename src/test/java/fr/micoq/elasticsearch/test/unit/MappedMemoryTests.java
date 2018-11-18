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
package fr.micoq.elasticsearch.test.unit;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.elasticsearch.index.store.EsBaseDirectoryTestCase;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;

import fr.micoq.elasticsearch.NativeUnixDirectory;
import fr.micoq.elasticsearch.NativeUnixDirectory.ForceIO;

/*
 * Tests the directory by using exclusively native mapped memory
 * for read access.
 */

@RunWith(RandomizedRunner.class)
public class MappedMemoryTests extends EsBaseDirectoryTestCase {
  
  @Override
  protected Directory getDirectory(Path file) throws IOException {
    return new NativeUnixDirectory(
        file,
        FSLockFactory.getDefault(),
        true,
        false,
        false,
        false,
        NativeUnixDirectory.DEFAULT_DIRECT_BUFFER_SIZE,
        NativeUnixDirectory.DEFAULT_DIRECT_BUFFER_SIZE,
        ForceIO.MappedMemory,
        NativeUnixDirectory.DEFAULT_MIN_BYTES_DIRECT,
        NativeUnixDirectory.DEFAULT_MAX_BYTES_PRELOAD,
        new HashSet<String>());
  }
}
