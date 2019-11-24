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

package io.cdap.cdap.app.etl.google.sheets;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.AppendCellsRequest;
import com.google.api.services.sheets.v4.model.AppendDimensionRequest;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.GridRange;
import com.google.api.services.sheets.v4.model.Request;
import com.google.api.services.sheets.v4.model.RowData;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.app.etl.google.GoogleBaseTest;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.proto.ProgramRunStatus;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests reading to and writing from Google Sheets within a sandbox cluster.
 */
public class GoogleSheetsTest extends GoogleBaseTest {
  protected static final ArtifactSelectorConfig GOOGLE_DRIVE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "google-drive-plugins", "[0.0.0, 100.0.0)");
  protected static final ArtifactSelectorConfig FILE_ARTIFACT =
    new ArtifactSelectorConfig("SYSTEM", "core-plugins", "[0.0.0, 100.0.0)");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String GOOGLE_SHEETS_PLUGIN_NAME = "GoogleSheets";
  private static final String FILE_PLUGIN_NAME = "File";
  private static final String TEST_SHEET_FILE_NAME = "sheetFile";
  private static final String TEXT_CSV_MIME = "text/csv";
  private static final String DEFAULT_SPREADSHEET_NAME = "sp0";
  private static final String DEFAULT_SHEET_NAME = "s0";
  private static final String TEST_TEXT_FILE_NAME = "textFile";

  public static final String TMP_FOLDER_NAME = "googleDriveTestFolder";

  private static Drive driveService;
  private static Sheets sheetsService;
  private String sourceFolderId;
  private String sinkFolderId;
  private String testSourceFileId;
  private String testSinkFileId;
  private Path tmpFolder;

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

    driveService = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
    sheetsService = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).build();
  }

  @Before
  public void testClassSetup() throws IOException {
    ImmutableList.of(ImmutableList.of(GOOGLE_SHEETS_PLUGIN_NAME, BatchSource.PLUGIN_TYPE, "cdap-data-pipeline"),
                     ImmutableList.of(GOOGLE_SHEETS_PLUGIN_NAME, BatchSink.PLUGIN_TYPE, "cdap-data-pipeline"))
      .forEach((pluginInfo) -> checkPluginExists(pluginInfo.get(0), pluginInfo.get(1), pluginInfo.get(2)));

    String sourceFolderName = RandomStringUtils.randomAlphanumeric(16);
    String sinkFolderName = RandomStringUtils.randomAlphanumeric(16);

    sourceFolderId = createFolder(driveService, sourceFolderName);
    sinkFolderId = createFolder(driveService, sinkFolderName);

    testSourceFileId = createFile(driveService, "".getBytes(), TEST_SHEET_FILE_NAME,
                                        "application/vnd.google-apps.spreadsheet", TEXT_CSV_MIME, sourceFolderId);
    tmpFolder = createFileSystemFolder(TMP_FOLDER_NAME);
  }


  @After
  public void removeFolders() throws IOException {
    if (testSourceFileId != null) {
      removeFile(driveService, testSourceFileId);
    }
    if (testSinkFileId != null) {
      removeFile(driveService, testSinkFileId);
    }
    if (sourceFolderId != null) {
      removeFile(driveService, sourceFolderId);
    }
    if (sinkFolderId != null) {
      removeFile(driveService, sinkFolderId);
    }

    Files.walk(tmpFolder)
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .forEach(java.io.File::delete);
  }

  @Test
  public void testSourceFileAllRecords() throws Exception {
    final int recordsToRead = 15;
    final int populatedRows = 10;
    Map<String, String> sourceProps = getSheetsSourceMinimalDefaultConfigs();
    sourceProps.put("lastDataRow", String.valueOf(recordsToRead));
    sourceProps.put("skipEmptyData", "false");
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId, generateSimpleRows(populatedRows, 5));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "GoogleSheetsSource", "FileSink",
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testSourceFileAllRecords");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsToRead);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(recordsToRead, fileLines.size());
    Assert.assertEquals(populatedRows, getNonNullRowsCount(fileLines));
  }

  @Test
  public void testSourceFileNonEmptyRecords() throws Exception {
    final int recordsToRead = 15;
    final int populatedRows = 10;
    Map<String, String> sourceProps = getSheetsSourceMinimalDefaultConfigs();
    sourceProps.put("lastDataRow", String.valueOf(recordsToRead));
    Map<String, String> sinkProps = getFileSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId, generateSimpleRows(populatedRows, 5));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "GoogleSheetsSource", "FileSink",
                        GOOGLE_SHEETS_PLUGIN_NAME, FILE_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, FILE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testSourceFileNonEmptyRecords");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, populatedRows);

    Assert.assertTrue(Files.isDirectory(tmpFolder));

    List<Path> allDeploysResults = Files.list(tmpFolder).collect(Collectors.toList());
    Assert.assertEquals(1, allDeploysResults.size());

    Path deployResult = allDeploysResults.get(0);
    Assert.assertTrue(Files.isDirectory(deployResult));
    Assert.assertEquals(1,
                        Files.list(deployResult).filter(p -> p.getFileName().toString().equals("_SUCCESS")).count());

    List<Path> destFiles =
      Files.list(deployResult).filter(p -> p.getFileName().toString().startsWith("part")).collect(Collectors.toList());
    Assert.assertEquals(1, destFiles.size());

    Path destFile = destFiles.get(0);
    List<String> fileLines = null;
    try {
      fileLines = Files.readAllLines(destFile);
    } catch (IOException e) {
      Assert.fail(String.format("Exception during reading file '%s': %s", destFile.toString(), e.getMessage()));
    }

    Assert.assertEquals(populatedRows, fileLines.size());
    Assert.assertEquals(populatedRows, getNonNullRowsCount(fileLines));
  }

  @Test
  public void testSourceSinkSingleFile() throws Exception {
    final int recordsPerName = 2;
    final int columnsNumber = 5;
    final List<String> names = Arrays.asList("name1", "name2");
    Map<String, String> sourceProps = getSheetsSourceMinimalDefaultConfigs();
    sourceProps.put("lastDataRow", String.valueOf(recordsPerName * names.size()));
    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateRowsWithNames(recordsPerName, columnsNumber, names));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "GoogleSheetsSource", "GoogleSheetsSink",
                        GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testSourceSinkSingleFile");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsPerName * names.size());

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, file.getName());

    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Assert.assertEquals(DEFAULT_SHEET_NAME, spreadsheet.getSheets().get(0).getProperties().getTitle());
    Assert.assertEquals(recordsPerName * names.size(),
                        spreadsheet.getSheets().get(0).getData().get(0).getRowData().size());
    for (RowData rowData : spreadsheet.getSheets().get(0).getData().get(0).getRowData()) {
      Assert.assertEquals(columnsNumber, rowData.getValues().size());
    }
  }

  @Test
  public void testSourceSinkSeparateFiles() throws Exception {
    final int recordsPerName = 2;
    final int columnsNumber = 5;
    final List<String> names = Arrays.asList("name1", "name2");
    Map<String, String> sourceProps = getSheetsSourceMinimalDefaultConfigs();
    sourceProps.put("lastDataRow", String.valueOf(recordsPerName * names.size()));
    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();

    // set first column value as file name
    sinkProps.put("schemaSpreadSheetNameFieldName", "A");

    // populate the sheet with simple rows
    populateSpreadSheetWithSimpleRows(sheetsService, testSourceFileId,
                                      generateRowsWithNames(recordsPerName, columnsNumber, names));

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "GoogleSheetsSource", "GoogleSheetsSink",
                        GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, GOOGLE_DRIVE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testSourceSinkSeparateFiles");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, recordsPerName * names.size());

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(2, resultFiles.size());

    for (File file : resultFiles) {
      Assert.assertTrue(names.contains(file.getName()));

      String fileId = file.getId();
      Spreadsheet spreadsheet = getSpreadsheet(fileId);

      Assert.assertTrue(names.contains(spreadsheet.getProperties().getTitle()));
      Assert.assertNotNull(spreadsheet.getSheets());
      Assert.assertEquals(1, spreadsheet.getSheets().size());
      Assert.assertEquals(DEFAULT_SHEET_NAME, spreadsheet.getSheets().get(0).getProperties().getTitle());
      Assert.assertEquals(recordsPerName,
                          spreadsheet.getSheets().get(0).getData().get(0).getRowData().size());
      for (RowData rowData : spreadsheet.getSheets().get(0).getData().get(0).getRowData()) {
        Assert.assertEquals(columnsNumber - 1, rowData.getValues().size());
      }
    }
  }

  @Test
  public void testFileSinkMergeCells() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                                   "{\"name\": \"test\",\"array\":[true,false]}");

    Map<String, String> sourceProps = getFileSourceMinimalDefaultConfigs();
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    schemaFields.add(Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    sourceProps.put("schema", fileSchema.toString());

    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();
    sinkProps.put("mergeDataCells", "true");

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "FileSource", "GoogleDriveSink",
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testFileSinkMergeCells");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check merges
    Assert.assertNotNull(sheet.getMerges());
    Assert.assertEquals(1, sheet.getMerges().size());

    // check data cells
    List<RowData> rows = sheet.getData().get(0).getRowData();
    Assert.assertEquals(2, rows.size());
    if (rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(1).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(1).getUserEnteredValue().getBoolValue());

      Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(2)
                            .setStartColumnIndex(0).setEndColumnIndex(1).setSheetId(sheet.getProperties().getSheetId()),
                          sheet.getMerges().get(0));
    } else if (rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(0).getUserEnteredValue().getBoolValue());

      Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(2)
                            .setStartColumnIndex(1).setEndColumnIndex(2).setSheetId(sheet.getProperties().getSheetId()),
                          sheet.getMerges().get(0));
    } else {
      Assert.fail("Invalid value for first cell of the first row in the result spreadSheet.");
    }
  }

  @Test
  public void testFileSinkFlattenCells() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                                   "{\"name\": \"test\",\"array\":[true,false]}");

    Map<String, String> sourceProps = getFileSourceMinimalDefaultConfigs();
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    schemaFields.add(Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    sourceProps.put("schema", fileSchema.toString());

    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "FileSource", "GoogleDriveSink",
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testFileSinkFlattenCells");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check cell values
    List<RowData> rows = sheet.getData().get(0).getRowData();
    Assert.assertEquals(2, rows.size());

    ExtendedValue firstValueInFirstRow = rows.get(0).getValues().get(0).getUserEnteredValue();
    if (firstValueInFirstRow.getStringValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(1).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(1).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(1).getUserEnteredValue().getBoolValue());
    } else if (rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue() != null) {
      Assert.assertEquals("test", rows.get(0).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(0).getValues().get(0).getUserEnteredValue().getBoolValue());
      Assert.assertEquals(false, rows.get(1).getValues().get(0).getUserEnteredValue().getBoolValue());
    } else {
      Assert.fail(String.format("Invalid value '%s' for first cell of the first row in the result spreadSheet.",
                                firstValueInFirstRow));
    }

    // check merges
    Assert.assertNull(sheet.getMerges());
  }

  @Test
  public void testFileSinkRecords() throws Exception {
    // create test file
    createFileSystemTextFile(tmpFolder, TEST_TEXT_FILE_NAME,
                                   "{\"record\":{\"field0\":true,\"field1\":\"test\"}}");

    Map<String, String> sourceProps = getFileSourceMinimalDefaultConfigs();
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("record", Schema.recordOf("record", Arrays.asList(
      Schema.Field.of("field0", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("field1", Schema.of(Schema.Type.STRING))
    ))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    sourceProps.put("schema", fileSchema.toString());

    Map<String, String> sinkProps = getSheetsSinkMinimalDefaultConfigs();
    sinkProps.put("writeSchema", "true");

    DeploymentDetails deploymentDetails =
      deployApplication(sourceProps, sinkProps, "FileSource", "GoogleDriveSink",
                        FILE_PLUGIN_NAME, GOOGLE_SHEETS_PLUGIN_NAME, FILE_ARTIFACT, GOOGLE_DRIVE_ARTIFACT,
                        GOOGLE_SHEETS_PLUGIN_NAME + "-testFileSinkFlattenCells");
    startWorkFlow(deploymentDetails.getAppManager(), ProgramRunStatus.COMPLETED);

    // check number of rows in and out
    checkRowsNumber(deploymentDetails, 1);

    List<File> resultFiles = getFiles(driveService, sinkFolderId);
    Assert.assertEquals(1, resultFiles.size());

    File file = resultFiles.get(0);
    String fileId = file.getId();
    Spreadsheet spreadsheet = getSpreadsheet(fileId);

    // check spreadSheet and sheet names
    Assert.assertEquals(DEFAULT_SPREADSHEET_NAME, spreadsheet.getProperties().getTitle());
    Assert.assertNotNull(spreadsheet.getSheets());
    Assert.assertEquals(1, spreadsheet.getSheets().size());
    Sheet sheet = spreadsheet.getSheets().get(0);
    Assert.assertEquals(DEFAULT_SHEET_NAME, sheet.getProperties().getTitle());

    // check cell values
    List<RowData> rows = sheet.getData().get(0).getRowData();
    // two rows for header and single for data
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("record", rows.get(0).getValues().get(0).getUserEnteredValue().getStringValue());
    Assert.assertNotNull(sheet.getMerges());
    Assert.assertEquals(1, sheet.getMerges().size());
    Assert.assertEquals(new GridRange().setStartRowIndex(0).setEndRowIndex(1)
                          .setStartColumnIndex(0).setEndColumnIndex(2).setSheetId(sheet.getProperties().getSheetId()),
                        sheet.getMerges().get(0));

    ExtendedValue firstSubHeaderValue = rows.get(1).getValues().get(0).getUserEnteredValue();
    if ("field0".equals(firstSubHeaderValue.getStringValue())) {
      Assert.assertEquals("field1", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(2).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(2).getValues().get(0).getUserEnteredValue().getBoolValue());
    } else if ("field1".equals(firstSubHeaderValue.getStringValue())) {
      Assert.assertEquals("field0", rows.get(1).getValues().get(1).getUserEnteredValue().getStringValue());
      Assert.assertEquals("test", rows.get(2).getValues().get(0).getUserEnteredValue().getStringValue());
      Assert.assertEquals(true, rows.get(2).getValues().get(1).getUserEnteredValue().getBoolValue());
    } else {
      Assert.fail(String.format("Invalid value '%s' for first sub-column name in the result spreadSheet.",
                                firstSubHeaderValue));
    }
  }

  private int getNonNullRowsCount(List<String> fileLines) {
    JsonParser jsonParser = new JsonParser();
    int notNullCount = 0;
    for (String line : fileLines) {
      JsonElement rootElement = jsonParser.parse(line);
      Assert.assertTrue(rootElement.isJsonObject());

      JsonObject rootObject = rootElement.getAsJsonObject();

      Assert.assertEquals(5, rootObject.entrySet().size());
      if (!rootObject.get("A").isJsonNull()) {
        notNullCount++;
      }
    }
    return notNullCount;
  }

  private Map<String, String> getSheetsSourceMinimalDefaultConfigs() {
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put("referenceName", "ref");
    sourceProps.put("directoryIdentifier", sourceFolderId);
    sourceProps.put("modificationDateRange", "lifetime");
    sourceProps.put("sheetsToPull", "all");

    sourceProps.put("extractMetadata", "false");
    sourceProps.put("metadataRecordName", "metadata");
    sourceProps.put("formatting", "formattedValues");
    sourceProps.put("skipEmptyData", "true");
    sourceProps.put("addNameFields", "false");
    sourceProps.put("columnNamesSelection", "noColumnNames");
    sourceProps.put("lastDataColumn", "5");
    sourceProps.put("lastDataRow", "5");
    sourceProps.put("readBufferSize", "5");

    sourceProps.put("authType", "oAuth2");
    sourceProps.put("clientId", getClientId());
    sourceProps.put("clientSecret", getClientSecret());
    sourceProps.put("refreshToken", getRefreshToken());
    sourceProps.put("maxRetryCount", "8");
    sourceProps.put("maxRetryWait", "200");
    sourceProps.put("maxRetryJitterWait", "100");
    return sourceProps;
  }

  private Map<String, String> getFileSourceMinimalDefaultConfigs() {
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
    Schema fileSchema = Schema.recordOf(
      "blob",
      schemaFields);
    Map<String, String> sourceProps = new HashMap<>();
    sourceProps.put("path", tmpFolder.toString());
    sourceProps.put("referenceName", "fileref");
    sourceProps.put("format", "json");
    sourceProps.put("schema", fileSchema.toString());
    return sourceProps;
  }

  private Map<String, String> getSheetsSinkMinimalDefaultConfigs() {
    Map<String, String> sinkProps = new HashMap<>();
    sinkProps.put("referenceName", "refd");
    sinkProps.put("directoryIdentifier", sinkFolderId);
    sinkProps.put("spreadSheetName", DEFAULT_SPREADSHEET_NAME);
    sinkProps.put("sheetName", DEFAULT_SHEET_NAME);
    sinkProps.put("writeSchema", "false");
    sinkProps.put("threadsNumber", "1");
    sinkProps.put("maxBufferSize", "10");
    sinkProps.put("recordsQueueLength", "100");
    sinkProps.put("maxFlushInterval", "10");
    sinkProps.put("flushExecutionTimeout", "100");
    sinkProps.put("minPageExtensionSize", "100");
    sinkProps.put("mergeDataCells", "false");
    sinkProps.put("skipNameFields", "true");

    sinkProps.put("authType", "oAuth2");
    sinkProps.put("clientId", getClientId());
    sinkProps.put("clientSecret", getClientSecret());
    sinkProps.put("refreshToken", getRefreshToken());
    sinkProps.put("maxRetryCount", "8");
    sinkProps.put("maxRetryWait", "200");
    sinkProps.put("maxRetryJitterWait", "100");
    return sinkProps;
  }

  private Map<String, String> getFileSinkMinimalDefaultConfigs() {
    Map<String, String> sinkProps = new HashMap<>();
    sinkProps.put("suffix", "yyyy-MM-dd-HH-mm");
    sinkProps.put("path", tmpFolder.toString());
    sinkProps.put("referenceName", "fileref");
    sinkProps.put("format", "json");
    return sinkProps;
  }

  private Spreadsheet getSpreadsheet(String spreadSheetId) throws IOException {
    Sheets.Spreadsheets.Get request = sheetsService.spreadsheets().get(spreadSheetId);
    request.setIncludeGridData(true);
    return request.execute();
  }

  private void extendRows(Sheets sheetsService, String fileId, int rowsNumber) throws IOException {
    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    requestBody.setRequests(new ArrayList<>());

    AppendDimensionRequest appendDimensionRequest = new AppendDimensionRequest();
    appendDimensionRequest.setLength(rowsNumber);
    appendDimensionRequest.setSheetId(getFirstAvailableSheetId(sheetsService, fileId));
    appendDimensionRequest.setDimension("ROWS");

    requestBody.getRequests().add(new Request().setAppendDimension(appendDimensionRequest));

    Sheets.Spreadsheets.BatchUpdate request =
      sheetsService.spreadsheets().batchUpdate(fileId, requestBody);

    request.execute();
  }

  private List<RowData> generateSimpleRows(int numberOfRows, int numberOfColumns) {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < numberOfRows; i++) {
      List<CellData> row = new ArrayList<>();
      for (int j = 0; j < numberOfColumns; j++) {
        row.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(i + "" + j)));
      }
      rows.add(new RowData().setValues(row));
    }
    return rows;
  }

  private List<RowData> generateRowsWithNames(int numberOfRowsPerName, int numberOfColumns, List<String> names) {
    List<RowData> rows = new ArrayList<>();
    for (int i = 0; i < numberOfRowsPerName; i++) {
      for (String name : names) {
        List<CellData> row = new ArrayList<>();
        for (int j = 0; j < numberOfColumns; j++) {
          row.add(new CellData().setUserEnteredValue(new ExtendedValue().setStringValue(name)));
        }
        rows.add(new RowData().setValues(row));
      }
    }
    return rows;
  }

  private void populateSpreadSheetWithSimpleRows(Sheets sheetsService, String fileId,
                                                 List<RowData> rowData) throws IOException {
    BatchUpdateSpreadsheetRequest requestBody = new BatchUpdateSpreadsheetRequest();
    requestBody.setRequests(new ArrayList<>());

    AppendCellsRequest appendCellsRequest = new AppendCellsRequest();
    appendCellsRequest.setFields("*");
    appendCellsRequest.setSheetId(getFirstAvailableSheetId(sheetsService, fileId));
    appendCellsRequest.setRows(rowData);

    requestBody.getRequests().add(new Request().setAppendCells(appendCellsRequest));

    Sheets.Spreadsheets.BatchUpdate request =
      sheetsService.spreadsheets().batchUpdate(fileId, requestBody);

    request.execute();
  }

  private Integer getFirstAvailableSheetId(Sheets sheetsService, String fileId) throws IOException {
    Spreadsheet spreadsheet = sheetsService.spreadsheets().get(fileId).execute();
    return spreadsheet.getSheets().get(0).getProperties().getSheetId();
  }
}
