package ru.naumen.gp.crawled;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CrawledIndexWriter implements IndexWriter {

    private static final Logger LOG = LoggerFactory
            .getLogger(MethodHandles.lookup().lookupClass());

    private Configuration configuration;
    private String apiUrl;
    private int batchSize;
    private String crawledSourceId;

    private CloseableHttpClient http;
    private Collection<NutchDocument> forAdd;
    private Collection<String> forDelete;
    private ObjectMapper jsonMapper = new ObjectMapper();

    private String getConfig(String key) {
        String value = configuration.get(key);
        if (value == null) {
            String msg = key + " not specified";
            LOG.error(msg);
            throw new RuntimeException(msg);
        } else {
            return value;
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;

        this.apiUrl = getConfig(CrawledConstants.SERVER_URL);
        this.crawledSourceId = getConfig(CrawledConstants.CRAWLED_SOURCE_ID);
        this.batchSize = configuration.getInt(CrawledConstants.BATCH_SIZE, 100);
        this.forAdd = new ArrayList<>(this.batchSize);
        this.forDelete = new ArrayList<>(this.batchSize);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void open(JobConf job, String name) throws IOException {
        LOG.info("Open indexer with apiUrl: {} and batchSize: {}", apiUrl, batchSize);
        http = HttpClients.createDefault();
        //TODO authorize in crawled?
    }

    private void add(NutchDocument doc) throws IOException {
        forAdd.add(doc);

        if (forAdd.size() >= batchSize) commit();
    }

    @Override
    public void write(NutchDocument doc) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("add doc {}", doc.getField("id"));
        }

        add(doc);
    }

    @Override
    public void delete(String key) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("delete doc with key {}", key);
        }

        forDelete.add(key);
        if (forDelete.size() >= batchSize) commit();
    }

    @Override
    public void update(NutchDocument doc) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("update doc {}", doc.getField("id"));
        }
        add(doc);
    }

    private void request(String path, String data) throws IOException {
        String url = apiUrl + path;
        HttpEntity entity = new StringEntity(data, StandardCharsets.UTF_8);

        HttpPost request = new HttpPost(url);
        //request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8");
        request.addHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8");
        request.setEntity(entity);

        try (CloseableHttpResponse response = http.execute(request)) {
            StatusLine status = response.getStatusLine();

            if (LOG.isTraceEnabled()) {
                LOG.trace("response status {}", status);
            }

            if (status.getStatusCode() >= HttpStatus.SC_BAD_REQUEST) {
                try (BufferedReader buf = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    String content = buf.lines().collect(Collectors.joining());
                    String msg = url + " : " + status.getReasonPhrase() + ". Description: " + content;
                    RuntimeException e = new RuntimeException(msg);
                    LOG.error("error when requesting crawled app", e);
                    throw e;
                }
            }
        }
    }

    private Collection<Map<String, Object>> prepareForAdd() {
        Collection<Map<String, Object>> res = new ArrayList<>(forAdd.size());
        for (NutchDocument doc : forAdd) {
            Map<String, Object> docMap = new HashMap<>();
            docMap.put("url", doc.getFieldValue("url"));
            LOG.warn(doc.getFieldValue("strippedContent").toString());
            docMap.put("content", doc.getFieldValue("strippedContent"));
            docMap.put("checksum", doc.getFieldValue("digest"));
            docMap.put("tstamp", doc.getFieldValue("tstamp"));
            docMap.put("sourceId", crawledSourceId);
            res.add(docMap);
        }
        return res;
    }

    @Override
    public void commit() throws IOException {
        if (!forAdd.isEmpty()) {
            LOG.info("trying to send docs to app");

            String json = jsonMapper.writeValueAsString(prepareForAdd());

            if (LOG.isTraceEnabled()) {
                LOG.trace(json);
            }

            request("/save", json);
            forAdd.clear();
        }

        if (!forDelete.isEmpty()) {
            LOG.info("trying to mark deleted docs for app");

            String json = jsonMapper.writeValueAsString(forDelete);

            if (LOG.isTraceEnabled()) {
                LOG.trace(json);
            }

            request("/markDeleted", json);
            forDelete.clear();
        }
    }

    @Override
    public void close() throws IOException {
        commit();
        http.close();
        LOG.info("end of indexing; close");
        //TODO goodbye to crawled?
    }

    @Override
    public String describe() {
        return "Crawled app API writer\n" +
                "\t" + CrawledConstants.SERVER_URL + ": url of API\n" +
                "\t" + CrawledConstants.BATCH_SIZE + ": size of buffer\n";
    }

}