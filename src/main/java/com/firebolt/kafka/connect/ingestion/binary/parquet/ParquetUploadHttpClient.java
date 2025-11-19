package com.firebolt.kafka.connect.ingestion.binary.parquet;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Slf4j
public class ParquetUploadHttpClient {

    private static final String FIXED_URL = "http://firebolt-core.local:3473/?database=integration_test_db";
    private static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");

    private final OkHttpClient httpClient;

    public ParquetUploadHttpClient() {
        this.httpClient = new OkHttpClient();
    }

    public void upload(String sql, String partName, byte[] parquetBytes) throws IOException {
        MultipartBody.Builder bodyBuilder = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("sql", sql)
                .addFormDataPart(partName, partName + ".parquet", RequestBody.create(parquetBytes, OCTET_STREAM));

        Request request = new Request.Builder()
                .url(FIXED_URL)
                .post(bodyBuilder.build())
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                String msg = response.body() != null ? response.body().string() : "";
                throw new IOException("Upload failed with HTTP " + response.code() + " " + response.message() + " " + msg);
            }
        }
    }
}


