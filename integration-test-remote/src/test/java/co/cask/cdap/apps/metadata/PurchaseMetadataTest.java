/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.apps.metadata;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordWritable;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.LineageClient;
import co.cask.cdap.client.MetadataClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.StreamViewClient;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.Lineage;
import co.cask.cdap.data2.metadata.lineage.LineageSerializer;
import co.cask.cdap.data2.metadata.lineage.Relation;
import co.cask.cdap.examples.purchase.PurchaseApp;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.artifact.PluginSummary;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.cdap.proto.metadata.lineage.LineageRecord;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.AudiTestBase;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Tests the metadata and lineage functionality using PurchaseApp
 */
public class PurchaseMetadataTest extends AudiTestBase {
  private static final Gson GSON = new GsonBuilder().
    registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();

  private static final Id.Application PURCHASE_APP = Id.Application.from(TEST_NAMESPACE, PurchaseApp.APP_NAME);
  private static final Id.Flow PURCHASE_FLOW = Id.Flow.from(PURCHASE_APP, "PurchaseFlow");
  private static final Id.Service PURCHASE_HISTORY_SERVICE = Id.Service.from(PURCHASE_APP, "PurchaseHistoryService");
  private static final Id.Service CATALOG_LOOKUP_SERVICE = Id.Service.from(PURCHASE_APP, "CatalogLookup");
  private static final Id.Service USER_PROFILE_SERVICE = Id.Service.from(PURCHASE_APP, "UserProfileService");
  private static final Id.Workflow PURCHASE_HISTORY_WORKFLOW = Id.Workflow.from(PURCHASE_APP,
                                                                                "PurchaseHistoryWorkflow");
  private static final Id.Program PURCHASE_HISTORY_BUILDER = Id.Program.from(PURCHASE_APP, ProgramType.MAPREDUCE,
                                                                             "PurchaseHistoryBuilder");
  private static final Id.Stream PURCHASE_STREAM = Id.Stream.from(Id.Namespace.DEFAULT, "purchaseStream");
  private static final Id.DatasetInstance HISTORY_DS = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "history");
  private static final Id.DatasetInstance PURCHASES_DS = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "purchases");
  private static final Id.DatasetInstance FREQUENT_CUSTOMERS_DS = Id.DatasetInstance.from(Id.Namespace.DEFAULT,
                                                                                          "frequentCustomers");
  private static final Id.DatasetInstance USER_PROFILES_DS = Id.DatasetInstance.from(Id.Namespace.DEFAULT,
                                                                                     "userProfiles");

  private static final Type SET_METADATA_RECORD_TYPE = new TypeToken<Set<MetadataRecord>>() { }.getType();
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();

  private MetadataClient metadataClient;
  private LineageClient lineageClient;

  @Before
  public void setup() {
    metadataClient = new MetadataClient(getClientConfig(), getRestClient());
    lineageClient = new LineageClient(getClientConfig(), getRestClient());
  }

  @Test
  public void testLineage() throws Exception {
    ProgramClient programClient = getProgramClient();

    ApplicationManager applicationManager = deployApplication(PurchaseApp.class);
    String streamName = "purchaseStream";

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long endTime = startTime + 10000;
    // assert no lineage for purchase stream.
    Assert.assertEquals(LineageSerializer.toLineageRecord(startTime, endTime, new Lineage(ImmutableSet.<Relation>of())),
                        lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // start PurchaseFlow and ingest an event
    FlowManager purchaseFlow = applicationManager.getFlowManager(PURCHASE_FLOW.getId()).start();
    purchaseFlow.waitForStatus(true, 60, 1);

    StreamManager purchaseStream = getTestManager().getStreamManager(Id.Stream.from(TEST_NAMESPACE, "purchaseStream"));
    purchaseStream.send("Milo bought 10 PBR for $12");

    RuntimeMetrics flowletMetrics = purchaseFlow.getFlowletMetrics("collector");
    flowletMetrics.waitForProcessed(1, 5, TimeUnit.MINUTES);

    Id.DatasetInstance dataset = Id.DatasetInstance.from(TEST_NAMESPACE, "purchases");
    Id.Stream stream = Id.Stream.from(TEST_NAMESPACE, streamName);

    List<RunRecord> ranRecords = getRunRecords(1, programClient, PURCHASE_FLOW,
                                               ProgramRunStatus.RUNNING.name(), 0, endTime);

    // check stream lineage
    LineageRecord expected =
      LineageSerializer.toLineageRecord(
        startTime, endTime,
        new Lineage(ImmutableSet.of(
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),

          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader")))
        )));
    Assert.assertEquals(expected, lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));
    WorkflowManager purchaseHistoryWorkflowManager =
      applicationManager.getWorkflowManager(PURCHASE_HISTORY_WORKFLOW.getId());
    MapReduceManager purchaseHistoryBuilderManager =
      applicationManager.getMapReduceManager(PURCHASE_HISTORY_BUILDER.getId());

    purchaseFlow.stop();
    purchaseFlow.waitForStatus(false, 60, 1);

    purchaseHistoryWorkflowManager.start();
    purchaseHistoryWorkflowManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(true, 60, 1);
    purchaseHistoryBuilderManager.waitForStatus(false, 10 * 60, 1);
    purchaseHistoryWorkflowManager.waitForStatus(false, 60, 1);

    ServiceManager purchaseHistoryService =
      applicationManager.getServiceManager(PURCHASE_HISTORY_SERVICE.getId());

    // add tag for the dataset
    metadataClient.addTags(HISTORY_DS, ImmutableSet.of("dsTag1"));

    // add tag for the service
    metadataClient.addTags(PURCHASE_HISTORY_SERVICE, ImmutableSet.of("serviceTag1"));

    // add metadata properties
    Map<String, String> serviceProperties = ImmutableMap.of("spKey1", "spValue1");
    metadataClient.addProperties(PURCHASE_HISTORY_SERVICE, serviceProperties);
    Map<String, String> appProperties = ImmutableMap.of("spKey1", "spApp1");
    metadataClient.addProperties(PURCHASE_APP, appProperties);

    String firstServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    Id.DatasetInstance historyDs = Id.DatasetInstance.from(TEST_NAMESPACE, "history");
    List<RunRecord> mrRanRecords = getRunRecords(1, programClient, PURCHASE_HISTORY_BUILDER,
                                                 ProgramRunStatus.COMPLETED.name(), 0, endTime);

    List<RunRecord> serviceRuns = getRunRecords(1, programClient, PURCHASE_HISTORY_SERVICE,
                                                ProgramRunStatus.KILLED.name(), 0, endTime);

    // lineage will have mapreduce and service relations now.
    expected =
      // When CDAP-3657 is fixed, we will no longer need to use LineageSerializer for serializing.
      // Instead we can direclty use Id.toString() to get the program and data keys.
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(0).getPid()))
        )));

    Assert.assertEquals(expected, lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // add more tags
    metadataClient.addTags(HISTORY_DS, ImmutableSet.of("dsTag2"));
    metadataClient.addTags(PURCHASE_HISTORY_SERVICE, ImmutableSet.of("serviceTag2"));

    // add more metadata props
    serviceProperties = ImmutableMap.of("spKey2", "spValue2");
    metadataClient.addProperties(PURCHASE_HISTORY_SERVICE, serviceProperties);

    String secondServiceRunId = makePurchaseHistoryServiceCallAndReturnRunId(purchaseHistoryService);

    serviceRuns = getRunRecords(2, programClient, PURCHASE_HISTORY_SERVICE,
                                ProgramRunStatus.KILLED.name(), 0, endTime);

    expected =
      LineageSerializer.toLineageRecord(
        startTime,
        endTime,
        new Lineage(ImmutableSet.of(
          new Relation(stream, PURCHASE_FLOW, AccessType.READ,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "reader"))),
          new Relation(dataset, PURCHASE_FLOW, AccessType.UNKNOWN,
                       RunIds.fromString(ranRecords.get(0).getPid()),
                       ImmutableSet.of(Id.Flow.Flowlet.from(PURCHASE_FLOW, "collector"))),
          new Relation(historyDs, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          new Relation(dataset, PURCHASE_HISTORY_BUILDER, AccessType.UNKNOWN,
                       RunIds.fromString(mrRanRecords.get(0).getPid())),
          // TODO : After CDAP-3623, the following will become one entry with runids in the set.
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(0).getPid())),
          new Relation(historyDs, PURCHASE_HISTORY_SERVICE, AccessType.UNKNOWN,
                       RunIds.fromString(serviceRuns.get(1).getPid()))
        )));

    Assert.assertEquals(expected, lineageClient.getLineage(PURCHASE_STREAM, startTime, endTime, null));

    // verify tags and metadata properties for the 2 service runs
    Set<MetadataRecord> expectedTagsFirst =
      ImmutableSet.of(
        new MetadataRecord(PURCHASE_APP, MetadataScope.USER, ImmutableMap.of("spKey1", "spApp1"),
                           ImmutableSet.<String>of()),
        new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER, ImmutableMap.of("spKey1", "spValue1"),
                           ImmutableSet.of("serviceTag1")),
        new MetadataRecord(historyDs, MetadataScope.USER, ImmutableMap.<String, String>of(),
                           ImmutableSet.of("dsTag1"))
      );

    verifyServiceRun(firstServiceRunId, expectedTagsFirst);

    Set<MetadataRecord> expectedTagsSecond = ImmutableSet.of(
      new MetadataRecord(PURCHASE_APP, MetadataScope.USER, ImmutableMap.of("spKey1", "spApp1"),
                         ImmutableSet.<String>of()),
      new MetadataRecord(PURCHASE_HISTORY_SERVICE, MetadataScope.USER,
                         ImmutableMap.of("spKey1", "spValue1", "spKey2", "spValue2"),
                         ImmutableSet.of("serviceTag1", "serviceTag2")),
      new MetadataRecord(historyDs, MetadataScope.USER,
                         ImmutableMap.<String, String>of(),
                         ImmutableSet.of("dsTag1", "dsTag2"))
    );

    verifyServiceRun(secondServiceRunId, expectedTagsSecond);

    // check dataset lineage
    Assert.assertEquals(expected, lineageClient.getLineage(HISTORY_DS, startTime, endTime, null));

    // verify search tags
    Set<MetadataSearchResultRecord> expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      );

    Assert.assertEquals(
      expectedSearchResults,
      metadataClient.searchMetadata(TEST_NAMESPACE, "service*", MetadataSearchTargetType.PROGRAM)
    );

    // search metadata properties
    expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      );
    Assert.assertEquals(
      expectedSearchResults,
      metadataClient.searchMetadata(TEST_NAMESPACE, "spKey1:spValue1", MetadataSearchTargetType.PROGRAM)
    );

    expectedSearchResults =
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(PURCHASE_APP)
      );

    Assert.assertEquals(
      expectedSearchResults,
      metadataClient.searchMetadata(TEST_NAMESPACE, "spKey1:sp*", MetadataSearchTargetType.ALL));
  }

  @Test
  public void testSearchUsingSystemMetadata() throws Exception {
    deployApplication(PurchaseApp.class);
    // search artifacts
    assertArtifactSearch();
    // search app
    assertAppSearch();
    // search programs
    assertProgramSearch();
    // search data entities
    assertDataEntitySearch();
  }

  private void assertArtifactSearch() throws Exception {
    // wait until cdap-etl-batch artifacts are available
    waitForETLBatchArtifact();
    String version = getMetaClient().getVersion().getVersion();
    Id.Artifact batchId = Id.Artifact.from(Id.Namespace.SYSTEM, "cdap-etl-batch", version);
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(batchId));
    Set<MetadataSearchResultRecord> result = metadataClient.searchMetadata(Id.Namespace.SYSTEM, "cdap-etl-batch", null);
    Assert.assertEquals(expected, result);
    result = metadataClient.searchMetadata(Id.Namespace.SYSTEM, "cdap-etl-b*", MetadataSearchTargetType.ARTIFACT);
    Assert.assertEquals(expected, result);
    ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());
    List<ArtifactSummary> allCorePlugins = artifactClient.listVersions(Id.Namespace.DEFAULT, "core-plugins",
                                                                       ArtifactScope.SYSTEM);
    Assert.assertTrue("Expected at least one core-plugins artifact.", allCorePlugins.size() > 0);
    String corePluginsVersion = allCorePlugins.get(0).getVersion();
    Id.Artifact corePlugins = Id.Artifact.from(Id.Namespace.SYSTEM, "core-plugins", corePluginsVersion);
    expected = ImmutableSet.of(new MetadataSearchResultRecord(corePlugins));
    result = metadataClient.searchMetadata(Id.Namespace.SYSTEM, "table", MetadataSearchTargetType.ARTIFACT);
    Assert.assertEquals(expected, result);
  }

  private void assertAppSearch() throws Exception {
    // using app name
    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_APP));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_APP.getId(), null));
    // using program names
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_FLOW.getId(),
                                                                MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_BUILDER.getId(),
                                                                MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_SERVICE.getId(),
                                                                MetadataSearchTargetType.APP));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_WORKFLOW.getId(),
                                                                MetadataSearchTargetType.APP));
    // using program types
    Assert.assertEquals(
      expected,
      metadataClient.searchMetadata(Id.Namespace.DEFAULT,
                                    ProgramType.FLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                                    MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected,
      metadataClient.searchMetadata(Id.Namespace.DEFAULT,
                                    ProgramType.MAPREDUCE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                                    MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected,
      metadataClient.searchMetadata(Id.Namespace.DEFAULT,
                                    ProgramType.SERVICE.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                                    MetadataSearchTargetType.APP));
    Assert.assertEquals(
      expected,
      metadataClient.searchMetadata(Id.Namespace.DEFAULT,
                                    ProgramType.WORKFLOW.getPrettyName() + MetadataDataset.KEYVALUE_SEPARATOR + "*",
                                    MetadataSearchTargetType.APP));

    // using schedule
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, "DailySchedule", null));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, "DataSchedule", null));
    Assert.assertEquals(expected, metadataClient.searchMetadata(Id.Namespace.DEFAULT, "1+MB", null));
  }

  private void assertProgramSearch() throws Exception {
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "batch", null));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, "realtime", null));

    // Using program names
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_FLOW.getId(), MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER),
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_BUILDER.getId(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_SERVICE.getId(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, CATALOG_LOOKUP_SERVICE.getId(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, USER_PROFILE_SERVICE.getId(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_HISTORY_WORKFLOW.getId(),
                                    MetadataSearchTargetType.PROGRAM));

    // using program types
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_FLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, ProgramType.FLOW.getPrettyName(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_BUILDER)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, ProgramType.MAPREDUCE.getPrettyName(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_SERVICE),
        new MetadataSearchResultRecord(CATALOG_LOOKUP_SERVICE),
        new MetadataSearchResultRecord(USER_PROFILE_SERVICE)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, ProgramType.SERVICE.getPrettyName(),
                                    MetadataSearchTargetType.PROGRAM));
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_HISTORY_WORKFLOW)
      ),
      metadataClient.searchMetadata(Id.Namespace.DEFAULT, ProgramType.WORKFLOW.getPrettyName(),
                                    MetadataSearchTargetType.PROGRAM));
  }

  private void assertDataEntitySearch() throws Exception {
    Id.Stream.View view = Id.Stream.View.from(PURCHASE_STREAM, "view");

    Set<MetadataSearchResultRecord> expected = ImmutableSet.of(
      new MetadataSearchResultRecord(PURCHASE_STREAM)
    );

    // schema search with fieldname
    Set<MetadataSearchResultRecord> result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "body", null);
    Assert.assertEquals(expected, result);

    // schema search with fieldname and fieldtype
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "body:" + Schema.Type.STRING.toString(), null);
    Assert.assertEquals(expected, result);

    // schema search for partial fieldname
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "bo*", null);
    Assert.assertEquals(expected, result);

    // schema search with fieldname and all/partial fieldtype
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "body:STR*", null);
    Assert.assertEquals(expected, result);

    // schema search for a field with the given fieldtype
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "string", null);
    Assert.assertEquals(
      ImmutableSet.of(
        new MetadataSearchResultRecord(PURCHASE_STREAM),
        new MetadataSearchResultRecord(HISTORY_DS),
        new MetadataSearchResultRecord(PURCHASES_DS)
      ), result);

    // create a view
    Schema viewSchema = Schema.recordOf("record",
                                        Schema.Field.of("viewBody", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    StreamViewClient viewClient = new StreamViewClient(getClientConfig(), getRestClient());
    viewClient.createOrUpdate(view, new ViewSpecification(new FormatSpecification("format", viewSchema)));

    // search all entities that have a defined schema
    // add a user property with "schema" as key
    Map<String, String> datasetProperties = ImmutableMap.of("schema", "schemaValue");
    metadataClient.addProperties(HISTORY_DS, datasetProperties);

    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "schema:*", null);
    Assert.assertEquals(ImmutableSet.<MetadataSearchResultRecord>builder()
                          .add(new MetadataSearchResultRecord(PURCHASE_STREAM))
                          .add(new MetadataSearchResultRecord(HISTORY_DS))
                          .add(new MetadataSearchResultRecord(PURCHASES_DS))
                          .add(new MetadataSearchResultRecord(view))
                          .build(),
                        result);

    // search dataset
    Set<MetadataSearchResultRecord> expectedKvTables = ImmutableSet.of(
      new MetadataSearchResultRecord(FREQUENT_CUSTOMERS_DS), new MetadataSearchResultRecord(USER_PROFILES_DS)
    );
    Set<MetadataSearchResultRecord> expectedBatchReadables = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedKvTables)
      .add(new MetadataSearchResultRecord(PURCHASES_DS))
      .build();
    Set<MetadataSearchResultRecord> expectedAllDatasets = ImmutableSet.<MetadataSearchResultRecord>builder()
      .addAll(expectedBatchReadables)
      .add(new MetadataSearchResultRecord(HISTORY_DS))
      .build();
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, BatchReadable.class.getSimpleName(), null);
    Assert.assertEquals(expectedBatchReadables, result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, BatchWritable.class.getSimpleName(),
                                           MetadataSearchTargetType.DATASET);
    Assert.assertEquals(expectedAllDatasets, result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, RecordScannable.class.getSimpleName(), null);
    Assert.assertEquals(expectedAllDatasets, result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, RecordWritable.class.getSimpleName(), null);
    Assert.assertEquals(expectedKvTables, result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, KeyValueTable.class.getName(), null);
    Assert.assertEquals(expectedKvTables, result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "type:*", null);
    Assert.assertEquals(expectedAllDatasets, result);

    // search using ttl
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "ttl:*", null);
    Assert.assertEquals(expected, result);

    // search using names. here purchase app gets matched because the stream name is in its schedule's description
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_STREAM.getId(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_STREAM),
                      new MetadataSearchResultRecord(view),
                      new MetadataSearchResultRecord(PURCHASE_APP)
      ),
      result);

    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_STREAM.getId(),
                                           MetadataSearchTargetType.STREAM);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(PURCHASE_STREAM)), result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASE_STREAM.getId(),
                                           MetadataSearchTargetType.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)), result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, "view", MetadataSearchTargetType.VIEW);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(view)), result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, HISTORY_DS.getId(), null);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(HISTORY_DS)), result);
    // here history dataset also gets matched because it has a field called 'purchases'
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, PURCHASES_DS.getId(), null);
    Assert.assertEquals(
      ImmutableSet.of(new MetadataSearchResultRecord(PURCHASES_DS),
                      new MetadataSearchResultRecord(HISTORY_DS)), result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, FREQUENT_CUSTOMERS_DS.getId(), null);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(FREQUENT_CUSTOMERS_DS)), result);
    result = metadataClient.searchMetadata(Id.Namespace.DEFAULT, USER_PROFILES_DS.getId(), null);
    Assert.assertEquals(ImmutableSet.of(new MetadataSearchResultRecord(USER_PROFILES_DS)), result);
  }

  private void waitForETLBatchArtifact() throws Exception {
    final ArtifactClient artifactClient = new ArtifactClient(getClientConfig(), getRestClient());
    String version = getMetaClient().getVersion().getVersion();
    final Id.Artifact batchId = Id.Artifact.from(Id.Namespace.DEFAULT, "cdap-etl-batch", version);
    // wait for the cdap-etl-batch artifact to be available
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          boolean batchReady = false;
          List<PluginSummary> plugins = artifactClient.getPluginSummaries(batchId, "batchsource", ArtifactScope.SYSTEM);
          for (PluginSummary plugin : plugins) {
            if ("Table".equals(plugin.getName())) {
              batchReady = true;
              break;
            }
          }
          return batchReady;
        } catch (ArtifactNotFoundException e) {
          // happens if etl-batch or etl-realtime were not added yet
          return false;
        }
      }
    }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
  }

  private void verifyServiceRun(String runId, Set<MetadataRecord> expected) throws Exception {
    URL serviceRunURL = getClientConfig().resolveNamespacedURLV3(
      TEST_NAMESPACE, String.format("apps/%s/services/%s/runs/%s/metadata",
                                    PURCHASE_APP.getId(), PURCHASE_HISTORY_SERVICE.getId(), runId));

    HttpResponse response = getRestClient().execute(HttpRequest.get(serviceRunURL).build(),
                                                    getClientConfig().getAccessToken());
    Assert.assertEquals(200, response.getResponseCode());

    Set<MetadataRecord> metadataRecords = GSON.fromJson(response.getResponseBodyAsString(),
                                                              SET_METADATA_RECORD_TYPE);

    Assert.assertEquals(expected, metadataRecords);
  }

  // starts service, makes a handler call, stops it and finally returns the runId of the completed run
  private String makePurchaseHistoryServiceCallAndReturnRunId(ServiceManager purchaseHistoryService) throws Exception {
    purchaseHistoryService.start();
    purchaseHistoryService.waitForStatus(true, 60, 1);

    URL historyURL = new URL(purchaseHistoryService.getServiceURL(), "history/Milo");

    // we have to make the first handler call after service starts with a retry
    retryRestCalls(HttpURLConnection.HTTP_OK, HttpRequest.get(historyURL).build(),
                   120, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

    List<RunRecord> runRecords = getRunRecords(1, getProgramClient(), PURCHASE_HISTORY_SERVICE,
                                               ProgramRunStatus.RUNNING.name(), 0, Long.MAX_VALUE);

    Assert.assertEquals(1, runRecords.size());
    purchaseHistoryService.stop();
    purchaseHistoryService.waitForStatus(false, 60, 1);
    return runRecords.get(0).getPid();
  }
}
