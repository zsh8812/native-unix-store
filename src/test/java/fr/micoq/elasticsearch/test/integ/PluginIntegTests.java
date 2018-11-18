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
package fr.micoq.elasticsearch.test.integ;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;
import org.junit.runner.RunWith;

import fr.micoq.elasticsearch.NativeUnixStorePlugin;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class PluginIntegTests extends ESIntegTestCase {
  
  private static String INDEX = "native_index";
  
  private Client client;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    this.client = client();
    closeAfterSuite(this.client);
  }
  
  private void makeIndex(boolean direct) {
    
    Builder builder = Settings.builder()
      .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),NativeUnixStorePlugin.STORE_TYPE)
      .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS,0)
      .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS,1);
    
    if(direct) {
      builder.put(NativeUnixStorePlugin.SETTING_DIRECT_READ_ENABLED.getKey(), true);
      builder.put(NativeUnixStorePlugin.SETTING_DIRECT_WRITE_ENABLED.getKey(), true);
    }
    
    Settings settings = builder.build();
    
    ElasticsearchAssertions.assertAcked(this.client.admin().indices().prepareCreate(INDEX).setSettings(settings).get());
  }
  
  private void makeContent() throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder()
      .startObject()
        .field("timestamp", new Date())
        .field("user", randomUnicodeOfLength(64))
        .field("id", randomInt())
        .field("value", randomDouble())
        .field("message", randomUnicodeOfLength(64))
      .endObject();

    BulkRequestBuilder bulkBuilder = this.client.prepareBulk();
    IndexRequestBuilder requestBuilder = this.client.prepareIndex().setIndex(INDEX).setSource(builder,XContentType.JSON);
    for(int i = 0; i < 10000; ++i) {
      bulkBuilder.add(requestBuilder.request());
    }
  }

  public void testCreateIndex() {
    makeIndex(false);
    ensureGreen();
    assertTrue(indexExists(INDEX));
  }
  
  public void testBulkAndMerge() throws IOException {
    makeIndex(false);
    makeContent();
    this.client.admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
  }
  
  public void testBulkAndMergeDirect() throws IOException {
    makeIndex(true);
    makeContent();
    this.client.admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
  }
  
}
