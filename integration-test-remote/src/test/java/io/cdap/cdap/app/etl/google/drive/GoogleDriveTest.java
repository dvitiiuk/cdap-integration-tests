/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.app.etl.google.drive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.PluginSummary;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.test.ApplicationManager;

/**
 * Tests reading to and writing from Google Drive within a Dataproc cluster.
 */
public class GoogleDriveTest extends DataprocUserCredentialsTestBase {
  protected static final ArtifactSelectorConfig GOOGLE_DRIVE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-drive-plugins", "[0.0.0, 100.0.0)");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String GOOGLE_DRIVE_PLUGIN_NAME = "GoogleDrive";
  private static final int GENERATED_NAME_LENGTH = 16;
  private static final String TEXT_PLAIN_MIME = "text/plain";
  private static final String TEXT_CSV_MIME = "text/csv";
  private static final String UNDEFINED_MIME = "application/octet-stream";

  private static final String TEST_TEXT_FILE_NAME = "textFile";
  private static final String TEST_DOC_FILE_NAME = "docFile";
  private static final String TEST_SHEET_FILE_NAME = "sheetFile";
  private static final String TEST_TEXT_FILE_CONTENT = "text file content";
  private static final String TEST_DOC_FILE_CONTENT = "Google Document file content";
  private static final String TEST_SHEET_FILE_CONTENT = "a,b,c\r\n,d,e";

  private static Drive service;
  private String sourceFolderId;
  private String sinkFolderId;
  private String testTextFileId;
  private String testDocFileId;
  private String testSheetFileId;

  @BeforeClass
  public static void setupDrive() throws GeneralSecurityException, IOException {
    final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

    GoogleCredential credential = new GoogleCredential.Builder()
      .setTransport(HTTP_TRANSPORT)
      .setJsonFactory(JSON_FACTORY)
      .setClientSecrets(getClientId(),
                        getClientSecret())
      .build();
    credential.setRefreshToken(getRefreshToken());

    service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
  }

  @Before
  public void testClassSetup() throws IOException {
    String sourceFolderName = RandomStringUtils.randomAlphanumeric(16);
    String sinkFolderName = RandomStringUtils.randomAlphanumeric(16);

    sourceFolderId = createFolder(service, sourceFolderName);
    sinkFolderId = createFolder(service, sinkFolderName);

    testTextFileId = createFile(service, TEST_TEXT_FILE_CONTENT.getBytes(), TEST_TEXT_FILE_NAME,
                                TEXT_PLAIN_MIME, null, sourceFolderId);
    testDocFileId = createFile(service, TEST_DOC_FILE_CONTENT.getBytes(), TEST_DOC_FILE_NAME,
                               "application/vnd.google-apps.document", TEXT_PLAIN_MIME, sourceFolderId);
    testSheetFileId = createFile(service, TEST_SHEET_FILE_CONTENT.getBytes(), TEST_SHEET_FILE_NAME,
                                 "application/vnd.google-apps.spreadsheet", TEXT_CSV_MIME, sourceFolderId);
  }

  @After
  public void removeFolders() throws IOException {
    removeFile(service, testTextFileId);
    removeFile(service, testDocFileId);
    removeFile(service, testSheetFileId);
    removeFile(service, sourceFolderId);
    removeFile(service, sinkFolderId);
  }

  @Test
  public void testBinaryOnly() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "binary");
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testBinaryOnly");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(1, destFiles.size());

    File textFile = destFiles.get(0);

    assertEquals(UNDEFINED_MIME, textFile.getMimeType());
    assertNotEquals(TEST_TEXT_FILE_NAME, textFile.getName());
    assertEquals(GENERATED_NAME_LENGTH, textFile.getName().length());

    String content = getFileContent(textFile.getId());
    assertEquals(TEST_TEXT_FILE_CONTENT, content);
  }

  @Test
  public void testDocFileOnly() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "documents");
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testDocFileOnly");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(1, destFiles.size());

    File docFile = destFiles.get(0);

    assertEquals(UNDEFINED_MIME, docFile.getMimeType());
    assertNotEquals(TEST_TEXT_FILE_NAME, docFile.getName());
    assertEquals(GENERATED_NAME_LENGTH, docFile.getName().length());

    String content = getFileContent(docFile.getId());
    // check BOM
    assertEquals('\uFEFF', content.charAt(0));
    assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
  }

  @Test
  public void testAllFileTypes() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "binary,documents,spreadsheets");
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testAllFileTypes");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      assertEquals(UNDEFINED_MIME, file.getMimeType());
      assertNotEquals(TEST_TEXT_FILE_NAME, file.getName());
      assertNotEquals(TEST_DOC_FILE_NAME, file.getName());
      assertNotEquals(TEST_SHEET_FILE_NAME, file.getName());
      assertEquals(GENERATED_NAME_LENGTH, file.getName().length());
    });
  }

  @Test
  public void testAllFileTypesNamed() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "binary,documents,spreadsheets");
    sourceProps.put("fileMetadataProperties", "name");
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();
    sinkProps.put("schemaNameFieldName", "name");

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testAllFileTypesNamed");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      assertEquals(UNDEFINED_MIME, file.getMimeType());
      Assert.assertNotNull(file.getName());
      try {
        String fileName = file.getName();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            assertEquals(TEST_TEXT_FILE_CONTENT, content);
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            assertEquals('\uFEFF', content.charAt(0));
            assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            break;
          case TEST_SHEET_FILE_NAME:
            assertEquals(TEST_SHEET_FILE_CONTENT, content);
            break;
          default:
            fail(String.format("Invalid file name after pipeline completion: '%s', content: '%s'", fileName, content));
        }
      } catch (IOException e) {
        fail(String.format("Exception during test results check: [%s]", e.getMessage()));
      }
    });
  }

  @Test
  public void testAllFileTypesNamedAndMimed() throws Exception {
    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "binary,documents,spreadsheets");
    sourceProps.put("fileMetadataProperties", "name,mimeType");
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();
    sinkProps.put("schemaNameFieldName", "name");
    sinkProps.put("schemaMimeFieldName", "mimeType");

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testAllFileTypesNamedAndMimed");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 3);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(3, destFiles.size());

    destFiles.forEach(file -> {
      Assert.assertNotNull(file.getName());
      String fileName = null;
      String mimeType = null;
      try {
        fileName = file.getName();
        mimeType = file.getMimeType();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            assertEquals(TEST_TEXT_FILE_CONTENT, content);
            assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            assertEquals('\uFEFF', content.charAt(0));
            assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_SHEET_FILE_NAME:
            assertEquals(TEST_SHEET_FILE_CONTENT, content);
            assertEquals(TEXT_CSV_MIME, mimeType);
            break;
          default:
            fail(String.format("Invalid file name after pipeline completion: '%s', content: '%s', mime type: '%s'",
                               fileName, content, mimeType));
        }
      } catch (IOException e) {
        fail(String.format("Exception during test results check: [%s], file name '%s', mimeType '%s'",
                           e.getMessage(),
                           fileName == null ? "unknown" : fileName,
                           mimeType == null ? "unknown" : mimeType));
      }
    });
  }

  @Test
  public void testPartitionSize() throws Exception {
    int testMaxPartitionSize = 10;

    Map<String, String> sourceProps = getSourceMinimalDefaultConfigs();
    sourceProps.put("fileTypesToPull", "binary,documents,spreadsheets");
    sourceProps.put("fileMetadataProperties", "name,mimeType");
    sourceProps.put("maxPartitionSize", Integer.toString(testMaxPartitionSize));
    Map<String, String> sinkProps = getSinkMinimalDefaultConfigs();
    sinkProps.put("schemaNameFieldName", "name");
    sinkProps.put("schemaMimeFieldName", "mimeType");

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps,
                        GOOGLE_DRIVE_PLUGIN_NAME + "-testAllFileTypesNamedAndMimed");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 4);

    List<File> destFiles = getFiles(sinkFolderId);
    assertEquals(4, destFiles.size());

    // flags to check partitioning work
    boolean firstTextPart = false;
    boolean secondTextPart = false;
    List<String> parts = new ArrayList<>();

    // Document and Sheets don't support partitioning
    for (File file : destFiles) {
      Assert.assertNotNull(file.getName());
      try {
        String fileName = file.getName();
        String mimeType = file.getMimeType();
        String content = getFileContent(file.getId());
        switch (fileName) {
          case TEST_TEXT_FILE_NAME:
            assertNotEquals(TEST_TEXT_FILE_CONTENT, content);
            assertEquals(TEXT_PLAIN_MIME, mimeType);
            parts.add(content);
            if (content.equals(TEST_TEXT_FILE_CONTENT.substring(0, testMaxPartitionSize))) {
              firstTextPart = true;
            }
            if (content.equals(TEST_TEXT_FILE_CONTENT.substring(testMaxPartitionSize))) {
              secondTextPart = true;
            }
            break;
          case TEST_DOC_FILE_NAME:
            // check BOM
            assertEquals('\uFEFF', content.charAt(0));
            assertEquals(TEST_DOC_FILE_CONTENT, content.replace("\uFEFF", ""));
            assertEquals(TEXT_PLAIN_MIME, mimeType);
            break;
          case TEST_SHEET_FILE_NAME:
            assertEquals(TEST_SHEET_FILE_CONTENT, content);
            assertEquals(TEXT_CSV_MIME, mimeType);
            break;
          default:
            fail(String.format("Invalid file name after pipeline completion: '%s', content: '%s', mime type: '%s'",
                               fileName, content, mimeType));
        }
      } catch (IOException e) {
        fail(String.format("Exception during test results check: [%s]", e.getMessage()));
      }
    }
    assertTrue(String.format("Text file was separated incorrectly: %s", parts.toString()), firstTextPart);
    assertTrue(String.format("Text file was separated incorrectly: %s", parts.toString()), secondTextPart);
  }

  private Map<String, String> getSourceMinimalDefaultConfigs() {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put("referenceName", "ref");
    sourceProps.put("directoryIdentifier", sourceFolderId);
    sourceProps.put("modificationDateRange", "lifetime");
    sourceProps.put("fileTypesToPull", "binary");
    sourceProps.put("maxPartitionSize", "0");
    sourceProps.put("bodyFormat", "bytes");
    sourceProps.put("docsExportingFormat", "text/plain");
    sourceProps.put("sheetsExportingFormat", "text/csv");
    sourceProps.put("drawingsExportingFormat", "image/svg+xml");
    sourceProps.put("presentationsExportingFormat", "text/plain");
    sourceProps.put("authType", "oAuth2");
    sourceProps.put("clientId", getClientId());
    sourceProps.put("clientSecret", getClientSecret());
    sourceProps.put("refreshToken", getRefreshToken());
    return sourceProps;
  }

  private Map<String, String> getSinkMinimalDefaultConfigs() {
    Map<String, String> sinkProps = new HashMap<>();
    sinkProps.put("referenceName", "refd");
    sinkProps.put("directoryIdentifier", sinkFolderId);
    sinkProps.put("schemaBodyFieldName", "body");
    sinkProps.put("authType", "oAuth2");
    sinkProps.put("clientId", getClientId());
    sinkProps.put("clientSecret", getClientSecret());
    sinkProps.put("refreshToken", getRefreshToken());
    return sinkProps;
  }

  private void checkRowsNumber(DeploymentDetails deploymentDetails, int expectedCount) throws Exception {
    ApplicationId appId = deploymentDetails.getAppId();
    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, appId.getNamespace(),
                                               Constants.Metrics.Tag.APP, appId.getEntityName());
    checkMetric(tags, "user." + deploymentDetails.getSource().getName() + ".records.out",
                expectedCount, 10);
    checkMetric(tags, "user." + deploymentDetails.getSink().getName() + ".records.in",
                expectedCount, 10);
  }

  @Override
  protected void innerSetup() {
    ImmutableList.of(ImmutableList.of(GOOGLE_DRIVE_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, "cdap-data-pipeline"),
                     ImmutableList.of(GOOGLE_DRIVE_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, "cdap-data-pipeline"))
      .forEach((pluginInfo) -> checkPluginExists(pluginInfo.get(0), pluginInfo.get(1), pluginInfo.get(2)));
  }

  void checkPluginExists(String pluginName, String pluginType, String artifact) {
    Preconditions.checkNotNull(pluginName);
    Preconditions.checkNotNull(pluginType);
    Preconditions.checkNotNull(artifact);

    try {
      Tasks.waitFor(true, () -> {
        try {
          final ArtifactId artifactId = TEST_NAMESPACE.artifact(artifact, version);
          List<PluginSummary> plugins =
            artifactClient.getPluginSummaries(artifactId, pluginType, ArtifactScope.SYSTEM);
          return plugins.stream().anyMatch(pluginSummary -> pluginName.equals(pluginSummary.getName()));
        } catch (ArtifactNotFoundException e) {
          // happens if the relevant artifact(s) were not added yet
          return false;
        }
      }, 5, TimeUnit.MINUTES, 3, TimeUnit.SECONDS);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void innerTearDown() throws Exception {
  }

  private DeploymentDetails deployApplication(Map<String, String> sourceProperties,
                                              Map<String, String> sinkProperties,
                                              String applicationName) throws Exception {
    ETLStage source = new ETLStage("GoogleDriveSource",
                                   new ETLPlugin(GOOGLE_DRIVE_PLUGIN_NAME,
                                                 BatchSource.PLUGIN_TYPE,
                                                 sourceProperties,
                                                 GOOGLE_DRIVE_ARTIFACT));
    ETLStage sink = new ETLStage("GoogleDriveSink", new ETLPlugin(GOOGLE_DRIVE_PLUGIN_NAME,
                                                                  BatchSink.PLUGIN_TYPE,
                                                                  sinkProperties,
                                                                  GOOGLE_DRIVE_ARTIFACT));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = getBatchAppRequestV2(etlConfig);
    ApplicationId appId = TEST_NAMESPACE.app(applicationName);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);
    return new DeploymentDetails(source, sink, appId, applicationManager);
  }

  private static String createFile(Drive service, byte[] content, String name, String mime, String subMime,
                                   String folderId) throws IOException {
    File fileToWrite = new File();
    fileToWrite.setName(name);
    fileToWrite.setParents(Collections.singletonList(folderId));
    fileToWrite.setMimeType(mime);
    ByteArrayContent fileContent = new ByteArrayContent(subMime, content);

    File file = service.files().create(fileToWrite, fileContent)
      .setFields("id, parents, mimeType")
      .execute();
    return file.getId();
  }

  private static String createFolder(Drive service, String folderName) throws IOException {
    File fileMetadata = new File();
    fileMetadata.setName(folderName);
    fileMetadata.setMimeType("application/vnd.google-apps.folder");

    File createdFolder = service.files().create(fileMetadata).setFields("id").execute();
    return createdFolder.getId();
  }

  private static void removeFile(Drive service, String fileId) throws IOException {
    service.files().delete(fileId).execute();
  }

  private List<File> getFiles(String parentFolderId) {
    try {
      List<File> files = new ArrayList<>();
      String nextToken = "";
      Drive.Files.List request = service.files().list()
        .setQ(String.format("'%s' in parents", parentFolderId))
        .setFields("nextPageToken, files(id, name, size, mimeType)");
      while (nextToken != null) {
        FileList result = request.execute();
        files.addAll(result.getFiles());
        nextToken = result.getNextPageToken();
        request.setPageToken(nextToken);
      }
      return files;
    } catch (IOException e) {
      throw new RuntimeException("Issue during retrieving summary for files.", e);
    }
  }

  private String getFileContent(String fileId) throws IOException {
    OutputStream outputStream = new ByteArrayOutputStream();
    Drive.Files.Get get = service.files().get(fileId);

    get.executeMediaAndDownloadTo(outputStream);
    return ((ByteArrayOutputStream) outputStream).toString();
  }

  private class DeploymentDetails {

    private final ApplicationId appId;
    private final ETLStage source;
    private final ETLStage sink;
    private final ApplicationManager appManager;

    DeploymentDetails(ETLStage source, ETLStage sink, ApplicationId appId, ApplicationManager appManager) {
      this.appId = appId;
      this.source = source;
      this.sink = sink;
      this.appManager = appManager;
    }

    public ApplicationId getAppId() {
      return appId;
    }

    public ETLStage getSource() {
      return source;
    }

    public ETLStage getSink() {
      return sink;
    }

    public ApplicationManager getAppManager() {
      return appManager;
    }
  }
}
