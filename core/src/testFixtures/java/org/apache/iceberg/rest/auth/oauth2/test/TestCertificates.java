/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest.auth.oauth2.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Generates test certificates and keystores at runtime. All materials are generated lazily (once
 * per JVM) and stored in a temp directory.
 */
public final class TestCertificates {

  private static final class Holder {
    private static final TestCertificates INSTANCE = new TestCertificates();
  }

  public static TestCertificates instance() {
    return Holder.INSTANCE;
  }

  private static final String KEYSTORE_PASSWORD = "s3cr3t";

  private final Path rsaPrivateKeyPkcs8Pem;
  private final Path rsaCertificatePem;

  // Keystores
  private final Path keyStoreP12;
  private final Path mockServerP12;

  private TestCertificates() {
    try {
      Path baseDir = Files.createTempDirectory("iceberg-test-certs");
      Runtime.getRuntime()
          .addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(baseDir.toFile())));

      // Generate RSA key pair and certificate
      KeyPair rsaKeyPair = generateRsaKeyPair();
      X509Certificate rsaCertificate = generateSelfSignedCertificate(rsaKeyPair);

      rsaPrivateKeyPkcs8Pem = baseDir.resolve("rsa_private_key_pkcs8.pem");
      writePkcs8Pem(rsaKeyPair.getPrivate(), rsaPrivateKeyPkcs8Pem);

      rsaCertificatePem = baseDir.resolve("rsa_certificate.pem");
      writeCertificatePem(rsaCertificate, rsaCertificatePem);

      // Generate PKCS#12 keystore from RSA key + certificate
      keyStoreP12 = baseDir.resolve("keystore.p12");
      writeKeyStore(rsaKeyPair.getPrivate(), rsaCertificate, keyStoreP12);

      // Generate mock server PKCS#12 keystore from MockServer's CA materials
      mockServerP12 = baseDir.resolve("mockserver.p12");
      writeMockServerKeyStore(mockServerP12);

    } catch (Exception e) {
      throw new RuntimeException("Failed to generate test certificates", e);
    }
  }

  /** RSA private key in PKCS#8 PEM format ({@code BEGIN PRIVATE KEY}). */
  public Path rsaPrivateKeyPkcs8Pem() {
    return rsaPrivateKeyPkcs8Pem;
  }

  /** Self-signed RSA certificate in PEM format. */
  public Path rsaCertificatePem() {
    return rsaCertificatePem;
  }

  /** PKCS#12 keystore containing the RSA private key and certificate (password: s3cr3t). */
  public Path keyStoreP12() {
    return keyStoreP12;
  }

  /** PKCS#12 keystore containing MockServer's CA certificate and private key (password: s3cr3t). */
  public Path mockServerP12() {
    return mockServerP12;
  }

  /** The keystore password used for all keystores: {@code s3cr3t}. */
  public String keyStorePassword() {
    return KEYSTORE_PASSWORD;
  }

  private static KeyPair generateRsaKeyPair() throws GeneralSecurityException {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair();
  }

  @SuppressWarnings("JavaUtilDate")
  private static X509Certificate generateSelfSignedCertificate(KeyPair keyPair)
      throws GeneralSecurityException, OperatorCreationException, IOException {
    long now = System.currentTimeMillis();
    Date notBefore = new Date(now);
    Date notAfter = new Date(now + 365L * 100 * 24 * 60 * 60 * 1000);
    X500Name issuer = new X500Name("CN=localhost");

    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            issuer, BigInteger.valueOf(now), notBefore, notAfter, issuer, keyPair.getPublic());

    GeneralNames subjectAltNames =
        new GeneralNames(
            new GeneralName[] {
              new GeneralName(GeneralName.dNSName, "localhost"),
              new GeneralName(GeneralName.iPAddress, "127.0.0.1"),
            });
    builder.addExtension(Extension.subjectAlternativeName, false, subjectAltNames);
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));

    ContentSigner signer =
        new JcaContentSignerBuilder("SHA256withRSA")
            .setProvider(new BouncyCastleProvider())
            .build(keyPair.getPrivate());
    X509CertificateHolder holder = builder.build(signer);
    return new JcaX509CertificateConverter()
        .setProvider(new BouncyCastleProvider())
        .getCertificate(holder);
  }

  private static void writePkcs8Pem(PrivateKey key, Path path) throws IOException {
    String pem = toPem("PRIVATE KEY", key.getEncoded());
    Files.writeString(path, pem);
  }

  private static void writeCertificatePem(X509Certificate certificate, Path path)
      throws IOException {
    try {
      String pem = toPem("CERTIFICATE", certificate.getEncoded());
      Files.writeString(path, pem);
    } catch (GeneralSecurityException e) {
      throw new IOException("Failed to encode certificate", e);
    }
  }

  private static String toPem(String type, byte[] encoded) {
    String base64 =
        Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(encoded);
    return "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
  }

  private static void writeKeyStore(PrivateKey privateKey, X509Certificate certificate, Path path)
      throws Exception {
    KeyStore ks = KeyStore.getInstance("PKCS12");
    ks.load(null, null);
    ks.setKeyEntry(
        "1", privateKey, KEYSTORE_PASSWORD.toCharArray(), new Certificate[] {certificate});
    try (OutputStream os = Files.newOutputStream(path)) {
      ks.store(os, KEYSTORE_PASSWORD.toCharArray());
    }
  }

  private static void writeMockServerKeyStore(Path path) throws Exception {
    X509Certificate cert;
    PrivateKey key;
    try (InputStream certStream =
        TestCertificates.class.getResourceAsStream(
            "/org/mockserver/socket/CertificateAuthorityCertificate.pem")) {
      if (certStream == null) {
        throw new IllegalStateException(
            "MockServer CA certificate not found on classpath. "
                + "Ensure org.mock-server:mockserver-netty is a dependency.");
      }
      cert = readCertificateFromPem(certStream);
    }
    try (InputStream keyStream =
        TestCertificates.class.getResourceAsStream(
            "/org/mockserver/socket/CertificateAuthorityPrivateKey.pem")) {
      if (keyStream == null) {
        throw new IllegalStateException(
            "MockServer CA private key not found on classpath. "
                + "Ensure org.mock-server:mockserver-netty is a dependency.");
      }
      key = readPrivateKeyFromPem(keyStream);
    }
    writeKeyStore(key, cert, path);
  }

  private static X509Certificate readCertificateFromPem(InputStream is) throws Exception {
    return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(is);
  }

  private static PrivateKey readPrivateKeyFromPem(InputStream is) throws Exception {
    try (java.io.Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        org.bouncycastle.openssl.PEMParser parser =
            new org.bouncycastle.openssl.PEMParser(reader)) {
      Object pemObject = parser.readObject();
      org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter converter =
          new org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter()
              .setProvider(new BouncyCastleProvider());
      if (pemObject instanceof org.bouncycastle.openssl.PEMKeyPair) {
        return converter.getPrivateKey(
            ((org.bouncycastle.openssl.PEMKeyPair) pemObject).getPrivateKeyInfo());
      } else if (pemObject instanceof org.bouncycastle.asn1.pkcs.PrivateKeyInfo) {
        return converter.getPrivateKey((org.bouncycastle.asn1.pkcs.PrivateKeyInfo) pemObject);
      }
      throw new IllegalStateException("Unexpected PEM object: " + pemObject.getClass());
    }
  }
}
