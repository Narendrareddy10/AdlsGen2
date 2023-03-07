package com.onetrust.adlsgen2.files;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeServiceProperties;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Base64;

@Slf4j
@Service
public class FileOperationsService {


    @Value("${upload-directory:${user.home}}")
    public String uploadDir;
    @Value("${accountfqdn}")
    public String accountFqdn;
    @Value("${client-id}")
    public String clientId;

    @Value("${tenant-id}")
    public String tenantId;
    @Value("${client-secret}")
    public String clientSecret;

    @Value("${client-token-end-point}")
    public String clientTokenEndPoint;

    @Value("${adls-folder-path}")
    public String adlsPath;
    public void uploadFile(final MultipartFile file,
                           final String genVersion){
        try {
            Path copyLocation = Paths.get(uploadDir + File.separator + StringUtils.cleanPath(file.getOriginalFilename()));
            Files.copy(file.getInputStream(), copyLocation, StandardCopyOption.REPLACE_EXISTING);
            String fileName = StringUtils.cleanPath(file.getOriginalFilename());
            if (StringUtils.hasLength(genVersion) && genVersion == "1") {
                saveFileToGen1(copyLocation.toFile().getAbsolutePath());
        }
            saveFileToGen2(/*copyLocation.toFile().getAbsolutePath()*/fileName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("File Not Found");
        }

    }

    private void saveFileToGen1(String fileName) throws IOException {

        ADLStoreClient adlStoreClient=getADLStoreClient();
        boolean dirExists=adlStoreClient.checkExists(adlsPath+fileName);
        if(dirExists){

        }else {
            adlStoreClient.createDirectory(adlsPath+fileName);
        }
        //OutputStream stream = adlStoreClient.createFile("/"+layer+"/"+folder+"/"+fileName+fileFormat, IfExists.OVERWRITE);
        OutputStream stream = adlStoreClient.createFile(adlsPath+fileName+".xlsx", IfExists.OVERWRITE);
        byte[] bytes = Files.readAllBytes(Path.of(fileName));

        stream.write(bytes);
        stream.close();


    }
    private void saveFileToGen2(final String fileName) {
        final DataLakeServiceClient dataLakeServiceClient = getDataLakeServiceClient();
       final DataLakeServiceProperties dataLakeServiceProperties= dataLakeServiceClient.getProperties();
       final DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.getFileSystemClient(/*fileName*/"loadnow");
       final DataLakeDirectoryClient directoryClient =fileSystemClient.getDirectoryClient(this.adlsPath);

    }

    private DataLakeServiceClient getDataLakeServiceClient(){
        final String accountName=this.accountFqdn;
        final String accountKey=this.clientId;
        //DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();

        StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, Base64.getEncoder().encodeToString(accountKey.getBytes()));

        TokenCredential tokenCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .tenantId(tenantId)
                .clientSecret(clientSecret)
                .build();

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder.credential(tokenCredential);
        //builder.credential(sharedKeyCredential);
        builder.endpoint("https://" + accountName + ".dfs.core.windows.net");
        DataLakeServiceClient dataLakeServiceClient = builder.buildClient();
        return dataLakeServiceClient;
    }
    private ADLStoreClient getADLStoreClient(){
        AccessTokenProvider clientTokenProvider = new ClientCredsTokenProvider(clientTokenEndPoint, clientId, clientSecret);

        ADLStoreClient adlStoreClient = ADLStoreClient.createClient(accountFqdn,clientTokenProvider);
        return adlStoreClient;
    }

    private ADLStoreClient getAdlsStoreClient() {
        /*final String accountFqdn = appSettingsService.getAccountfqdn();
        final String clientTokenEndPoint = appSettingsService.getClienttokenendpoint();
        final String clientId = appSettingsService.getServicePrincipalId();
        final String clientSecret = appSettingsService.getServicePrincipalKey();
        */
        final AccessTokenProvider clientTokenProvider = new ClientCredsTokenProvider(clientTokenEndPoint, clientId, clientSecret);
        return ADLStoreClient.createClient(accountFqdn, clientTokenProvider);
    }

    public String storeFile(MultipartFile file) {
        log.info("Entering storeFile method in FileStorageService ..........");
        // Normalize file name
        String fileName = StringUtils.cleanPath(file.getOriginalFilename());

        try {
            // Check if the file's name contains invalid characters
            if(fileName.contains("..")) {
                throw new RuntimeException("Sorry! Filename contains invalid path sequence " + fileName);
            }

            // Copy file to the target location (Replacing existing file with the same name)
            //Path targetLocation = this.path.resolve(fileName);
            Path targetLocation=Paths.get(fileName);
            //Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);
            Files.write(targetLocation, file.getBytes());


            log.info("Exiting storeFile method in FileStorageService :");

            return fileName;
        } catch (IOException ex) {
            throw new RuntimeException("Could not store file " + fileName + ". Please try again!", ex);
        }
    }
}
