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

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.IndexStorePlugin;


public class NativeUnixStorePlugin extends Plugin implements IndexStorePlugin {

  public static final String STORE_TYPE = "nativeunixfs";
  
  public static final Setting<ByteSizeValue>SETTING_DIRECT_READ_BUFFER_SIZE =
      Setting.memorySizeSetting("index.store.direct.read.buffer_size",
      new ByteSizeValue(128,ByteSizeUnit.KB), Property.IndexScope, Property.Dynamic);
  public static final Setting<ByteSizeValue> SETTING_DIRECT_WRITE_BUFFER_SIZE =
      Setting.memorySizeSetting("index.store.direct.write.buffer_size",
      new ByteSizeValue(128,ByteSizeUnit.KB), Property.IndexScope, Property.Dynamic);
  public static final Setting<Boolean> SETTING_DIRECT_READ_ENABLED =
      Setting.boolSetting("index.store.direct.read.enabled", false, Property.IndexScope, Property.Dynamic);
  public static final Setting<Boolean> SETTING_DIRECT_WRITE_ENABLED =
      Setting.boolSetting("index.store.direct.write.enabled", false, Property.IndexScope, Property.Dynamic);
  public static final Setting<ByteSizeValue>SETTING_DIRECT_MIN_MERGE_SIZE =
      Setting.memorySizeSetting("index.store.direct.min_merge_size",
      new ByteSizeValue(10,ByteSizeUnit.MB), Property.IndexScope, Property.Dynamic);
  public static final Setting<Boolean> SETTING_MMAP_ENABLED =
      Setting.boolSetting("index.store.mmap.enabled", true, Property.IndexScope, Property.Dynamic);
  public static final Setting<Boolean> SETTING_MMAP_READ_AHEAD =
      Setting.boolSetting("index.store.mmap.read_ahead", false, Property.IndexScope, Property.Dynamic);
  public static final Setting<ByteSizeValue> SETTING_MMAP_MAX_PRELOAD_SIZE =
      Setting.byteSizeSetting("index.store.mmap.max_preload_size",
      new ByteSizeValue(0,ByteSizeUnit.BYTES), Property.IndexScope, Property.Dynamic);
  
  public NativeUnixStorePlugin(Settings settings) {
  }
  
  /*@Override
  public void onIndexModule(IndexModule indexModule) {
    indexModule.addIndexStore(STORE_TYPE, (settings)-> new NativeUnixIndexStore(settings));
  }*/

  @Override
  public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories() {
    final Map<String, Function<IndexSettings, IndexStore>> indexStoreFactories = new HashMap<>(1);
    indexStoreFactories.put(STORE_TYPE, NativeUnixIndexStore::new);
    return Collections.unmodifiableMap(indexStoreFactories);
  }
  
  @Override
  public List<Setting<?>> getSettings()
  {
    List<Setting<?>> sets = new ArrayList<Setting<?>>();
    sets.add(SETTING_DIRECT_READ_BUFFER_SIZE);
    sets.add(SETTING_DIRECT_WRITE_BUFFER_SIZE);
    sets.add(SETTING_DIRECT_READ_ENABLED);
    sets.add(SETTING_DIRECT_WRITE_ENABLED);
    sets.add(SETTING_DIRECT_MIN_MERGE_SIZE);
    sets.add(SETTING_MMAP_READ_AHEAD);
    sets.add(SETTING_MMAP_ENABLED);
    sets.add(SETTING_MMAP_MAX_PRELOAD_SIZE);
    return sets;
  }
}
