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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.etl.gcp.DataprocETLTestBase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Base64;

/**
 * An abstract class used for running integration tests with Google OAuth2 user credentials.
 */
public abstract class DataprocUserCredentialsTestBase extends DataprocETLTestBase {
  private static String clientId;
  private static String clientSecret;
  private static String refreshToken;

  @BeforeClass
  public static void testDataprocClassSetup() throws IOException {
    DataprocETLTestBase.testDataprocClassSetup();
    String clientIdProperty = System.getProperty("google.application.clientId.base64.encoded");
    String clientSecretProperty = System.getProperty("google.application.clientSecret.base64.encoded");
    String refreshTokenProperty = System.getProperty("google.application.refreshToken.base64.encoded");
    if (clientIdProperty == null || clientSecretProperty== null || refreshTokenProperty == null) {
      throw new IllegalArgumentException("Invalid user credential parameters");
    }

    clientId = Bytes.toString(Base64.getDecoder().decode(clientIdProperty));
    clientSecret = Bytes.toString(Base64.getDecoder().decode(clientSecretProperty));
    refreshToken = Bytes.toString(Base64.getDecoder().decode(refreshTokenProperty));
  }

  public static String getClientId() {
    return clientId;
  }

  public static String getClientSecret() {
    return clientSecret;
  }

  public static String getRefreshToken() {
    return refreshToken;
  }
}
