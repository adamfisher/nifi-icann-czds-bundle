package com.github.adamfisher.nifi.processors.czds;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.icann.czds.sdk.client.ZoneDownloadClient;
import org.icann.czds.sdk.model.ApplicationConstants;
import org.icann.czds.sdk.model.AuthenticationException;
import org.icann.czds.sdk.model.ClientConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;

public class NiFiZoneDownloadClient extends ZoneDownloadClient {

    public NiFiZoneDownloadClient(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Set<String> getDownloadURLs() throws IOException, AuthenticationException {
        try {
            authenticateIfRequired();
            String linksURL = getCzdsDownloadUrl() + ApplicationConstants.CZDS_LINKS;
            HttpResponse response = makeGetRequest(linksURL);

            if(response.getEntity().getContentLength() != 0)
                return objectMapper.readValue(response.getEntity().getContent(), Set.class);

        } catch (AuthenticationException | IOException e) {
            throw e;
        }

        return new HashSet<>();
    }

    public File getZoneFile(String downloadURL) throws IOException, AuthenticationException {
        HttpResponse response = makeGetRequest(downloadURL);
        return createFileLocally(response.getEntity().getContent(), getFileName(response));
    }

    private String getFileName(HttpResponse response) throws AuthenticationException {
        Header[] headers = response.getHeaders("Content-disposition");
        String preFileName = "attachment;filename=";
        if (headers.length == 0) {
            throw new AuthenticationException("ERROR: Either you are not authorized to download zone file of tld or tld does not exist");
        }
        String fileName = headers[0].getValue().substring(headers[0].getValue().indexOf(preFileName) + preFileName.length());
        return fileName;
    }

    private File createFileLocally(InputStream inputStream, String fileName) throws IOException {
        System.out.println("Saving zone file to " + fileName);
        File tempDirectory = new File(getZonefileOutputDirectory());
        if (!tempDirectory.exists()) {
            tempDirectory.mkdir();
        }

        File file = new File(getZonefileOutputDirectory(), fileName);
        try {
            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);

            inputStream.close();
            return file;
        } catch (IOException e) {
            throw new IOException("ERROR: Failed to save file to " + file.getAbsolutePath(), e);
        }
    }
}