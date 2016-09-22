// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3EncryptionClient;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;

/**
 * Abstract class of AmazonS3Client Producer.
 */
public abstract class AbstractAmazonS3ClientProducer {

    /**
     * Method to create {@link AmazonS3Client} depending on connection properties set during design time.
     */
    public AmazonS3Client createClient(AwsS3ConnectionProperties connectionProperties) throws IOException {
        AWSCredentialsProvider credProvider = createCredentialsProvider(connectionProperties);
        boolean isClientConfig = connectionProperties.configClient.getValue();
        Map<AwsS3ClientConfigFields, Object> clientConfigData = null;
        if (isClientConfig) {
            clientConfigData = connectionProperties.configClientTable.getConfig();
        }

        ClientConfiguration clientConfig = null;
        if (clientConfigData != null && !clientConfigData.isEmpty()) {
            clientConfig = ClientConfigurationBuilder.createClientConfiguration(clientConfigData);
        }

        if (connectionProperties.assumeRole.getValue()) {
            credProvider = assumeRoleCredentials(credProvider, connectionProperties, clientConfig);
        }

        AmazonS3Client client = null;
        if (clientConfig != null) {
            client = doCreateClientWithClientConfig(credProvider, clientConfig, connectionProperties);
        } else {
            client = doCreateClient(credProvider, connectionProperties);
        }
        if (connectionProperties.isRegionSet()) {
            client.setRegion(RegionUtils.getRegion(connectionProperties.region.getValue().getAwsRegionCode()));
        }
        return client;
    }

    private AWSCredentialsProvider createCredentialsProvider(AwsS3ConnectionProperties connectionProperties) {
        AWSCredentialsProvider credProvider;
        if (connectionProperties.inheritFromAwsRole.getValue()) {
            credProvider = new InstanceProfileCredentialsProvider();
        } else {
            credProvider = new StaticCredentialsProvider(
                    new BasicAWSCredentials(connectionProperties.accessSecretKeyProperties.accessKey.getValue(),
                            connectionProperties.accessSecretKeyProperties.secretKey.getValue()));
        }
        return credProvider;
    }

    /**
     * Method to create {@link AWSCredentialsProvider} if Assume roles feature is used.
     * 
     * @param credProvider - {@link AWSCredentialsProvider} containing access properties.
     * {@link StaticCredentialsProvider} for properties, when access and secret key are used, or
     * {@link InstanceProfileCredentialsProvider} if instance roles feature is setup.
     * @param connectionProperties - connection properties, set during the design time.
     * @param clientConfig - client configuration or null, if it is not used for this connection.
     */
    private AWSCredentialsProvider assumeRoleCredentials(AWSCredentialsProvider credProvider,
            AwsS3ConnectionProperties connectionProperties, ClientConfiguration clientConfig) {
        STSAssumeRoleSessionCredentialsProvider.Builder assumeRoleBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(
                connectionProperties.assumeRoleProps.arn.getValue(),
                connectionProperties.assumeRoleProps.roleSessionName.getValue()).withLongLivedCredentialsProvider(credProvider)
                        .withRoleSessionDurationSeconds(connectionProperties.assumeRoleProps.getSessionDurationSeconds());

        if (connectionProperties.setStsEndpoint.getValue()) {
            assumeRoleBuilder.withServiceEndpoint(connectionProperties.stsEndpoint.getValue());
        }

        if (clientConfig != null) {
            assumeRoleBuilder.withClientConfiguration(clientConfig);
        }

        return assumeRoleBuilder.build();
    }

    /**
     * Method used to create {@link AmazonS3Client} if Client Configuration properties were not set.
     * 
     * @param credentialsProvider - {@link AWSCredentialsProvider} containing access properties.
     * {@link StaticCredentialsProvider} for properties, when access and secret key are used,
     * {@link InstanceProfileCredentialsProvider} if instance roles feature is setup, or
     * {@link STSAssumeRoleSessionCredentialsProvider} if assume roles feature is setup.
     * @param connectionProperties - {@link AwsS3ConnectionProperties} set during design time.
     */
    protected abstract AmazonS3Client doCreateClient(AWSCredentialsProvider credentialsProvider,
            AwsS3ConnectionProperties connectionProperties) throws IOException;

    /**
     * Method used to create {@link AmazonS3Client} if Client Configuration properties were set.
     * 
     * @param credentialsProvider - {@link AWSCredentialsProvider} containing access properties.
     * {@link StaticCredentialsProvider} for properties, when access and secret key are used,
     * {@link InstanceProfileCredentialsProvider} if instance roles feature is setup, or
     * {@link STSAssumeRoleSessionCredentialsProvider} if assume roles feature is setup.
     * @param clientConfig - client configuration or null, if it is not used for this connection.
     * @param connectionProperties - {@link AwsS3ConnectionProperties} set during design time.
     */
    protected abstract AmazonS3Client doCreateClientWithClientConfig(AWSCredentialsProvider credentialsProvider,
            ClientConfiguration clientConfig, AwsS3ConnectionProperties connectionProperties) throws IOException;

    /**
     * AmazonS3ClientProducer class for non encrypted connection. Used when "Encryption" property of connection
     * properties is not checked.
     */
    public static class NonEncryptedAmazonS3ClientProducer extends AbstractAmazonS3ClientProducer {

        @Override
        protected AmazonS3Client doCreateClient(AWSCredentialsProvider credentialsProvider,
                AwsS3ConnectionProperties connectionProperties) {
            return new AmazonS3Client(credentialsProvider);
        }

        @Override
        protected AmazonS3Client doCreateClientWithClientConfig(AWSCredentialsProvider credentialsProvider,
                ClientConfiguration clientConfig, AwsS3ConnectionProperties connectionProperties) {
            return new AmazonS3Client(credentialsProvider, clientConfig);
        }

    }

    /**
     * AmazonS3ClientProducer class for encrypted connection. Used when "Encryption" property of connection properties
     * is checked.
     */
    protected abstract static class EncryptedAmazonS3ClientProducer extends AbstractAmazonS3ClientProducer {

        @Override
        protected AmazonS3EncryptionClient doCreateClient(AWSCredentialsProvider credentialsProvider,
                AwsS3ConnectionProperties connectionProperties) throws IOException {
            return new AmazonS3EncryptionClient(credentialsProvider, createEncryptionMaterials(connectionProperties),
                    createCryptoConfig(connectionProperties));
        }

        @Override
        protected AmazonS3Client doCreateClientWithClientConfig(AWSCredentialsProvider credentialsProvider,
                ClientConfiguration clientConfig, AwsS3ConnectionProperties connectionProperties) throws IOException {
            return new AmazonS3EncryptionClient(credentialsProvider, createEncryptionMaterials(connectionProperties),
                    clientConfig, createCryptoConfig(connectionProperties));
        }

        /**
         * Create {@link CryptoConfiguration} for AmazonS3Client using the {@link AwsS3ConnectionProperties} set during
         * the design time.
         * 
         * @param connectionProperties - {@link AwsS3ConnectionProperties} set during the design time.
         */
        protected CryptoConfiguration createCryptoConfig(AwsS3ConnectionProperties connectionProperties) {
            return new CryptoConfiguration();
        }

        /**
         * Create {@link EncryptionMaterialsProvider} for AmazonS3Client using the {@link AwsS3ConnectionProperties} set
         * during the design time.
         * 
         * @param connectionProperties - {@link AwsS3ConnectionProperties} set during the design time.
         */
        protected abstract EncryptionMaterialsProvider createEncryptionMaterials(AwsS3ConnectionProperties connectionProperties)
                throws IOException;

    }

    /**
     * AmazonS3ClientProducer class for encrypted connection and selected "KMS CMK" {@link EncryptionKeyType}.
     */
    public static class KmsCmkEncryptionAmazonS3ClientProducer extends EncryptedAmazonS3ClientProducer {

        @Override
        protected CryptoConfiguration createCryptoConfig(AwsS3ConnectionProperties connectionProperties) {
            CryptoConfiguration cryptoConfig = super.createCryptoConfig(connectionProperties);
            if (connectionProperties.isRegionSet()) {
                cryptoConfig.withAwsKmsRegion(RegionUtils.getRegion(connectionProperties.region.getValue().getAwsRegionCode()));
            }
            return cryptoConfig;
        }

        @Override
        protected EncryptionMaterialsProvider createEncryptionMaterials(AwsS3ConnectionProperties connectionProperties) {
            return new KMSEncryptionMaterialsProvider(connectionProperties.encryptionProperties.kmsCmkProperties.key.getValue());
        }

    }

    /**
     * AmazonS3ClientProducer class for encrypted connection and selected "SYMMETRIC_MASTER_KEY"
     * {@link EncryptionKeyType}.
     */
    public static class SymmetricKeyEncryptionAmazonS3ClientProvider extends EncryptedAmazonS3ClientProducer {

        @Override
        protected EncryptionMaterialsProvider createEncryptionMaterials(AwsS3ConnectionProperties connectionProperties)
                throws IOException {
            SecretKeySpec symmetricKey = null;
            AwsS3SymmetricKeyEncryptionProperties encryptionParameters = connectionProperties.encryptionProperties.symmetricKeyProperties;
            if (encryptionParameters.encoding.getValue() == Encoding.BASE_64) {
                symmetricKey = new SecretKeySpec(Base64.decodeBase64(encryptionParameters.key.getValue().getBytes("UTF-8")),
                        "AES");
            } else if (encryptionParameters.encoding.getValue() == Encoding.X509) {
                File keyFile = new File(encryptionParameters.keyFilePath.getValue());
                FileInputStream keyInputStream = null;
                try {
                    keyInputStream = new FileInputStream(keyFile);
                    byte[] encodedPrivateKey = new byte[(int) keyFile.length()];
                    keyInputStream.read(encodedPrivateKey);
                    symmetricKey = new SecretKeySpec(encodedPrivateKey, "AES");
                } finally {
                    if (keyInputStream != null) {
                        keyInputStream.close();
                    }
                }
            }
            EncryptionMaterials encryptionMaterials = new EncryptionMaterials(symmetricKey);
            return new StaticEncryptionMaterialsProvider(encryptionMaterials);
        }

    }

    /**
     * AmazonS3ClientProducer class for encrypted connection and selected "ASYMMETRIC_MASTER_KEY"
     * {@link EncryptionKeyType}.
     */
    public static class AsymmetricKeyEncryptionAmazonS3ClientProvider extends EncryptedAmazonS3ClientProducer {

        @Override
        protected EncryptionMaterialsProvider createEncryptionMaterials(AwsS3ConnectionProperties connectionProperties)
                throws IOException {
            AwsS3AsymmetricKeyEncryptionProperties encryptionProps = connectionProperties.encryptionProperties.asymmetricKeyProperties;
            File filePublicKey = new File(encryptionProps.publicKeyFilePath.getValue());
            FileInputStream fis = null;
            byte[] encodedPublicKey = null;
            try {
                fis = new FileInputStream(filePublicKey);
                encodedPublicKey = new byte[(int) filePublicKey.length()];
                fis.read(encodedPublicKey);
            } finally {
                if (fis != null) {
                    fis.close();
                    fis = null;
                }
            }

            File filePrivateKey = new File(encryptionProps.privateKeyFilePath.getValue());
            byte[] encodedPrivateKey = null;
            try {
                fis = new FileInputStream(filePrivateKey);
                encodedPrivateKey = new byte[(int) filePrivateKey.length()];
                fis.read(encodedPrivateKey);
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }

            KeyFactory keyFactory;
            KeyPair asymmetricKey = null;
            try {
                keyFactory = KeyFactory.getInstance(encryptionProps.algorithm.getValue().toString());

                X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
                PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

                PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
                PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);

                asymmetricKey = new KeyPair(publicKey, privateKey);
            } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                throw new IOException(e);
            }
            EncryptionMaterials encryptionMaterials = new EncryptionMaterials(asymmetricKey);
            return new StaticEncryptionMaterialsProvider(encryptionMaterials);
        }

    }

}