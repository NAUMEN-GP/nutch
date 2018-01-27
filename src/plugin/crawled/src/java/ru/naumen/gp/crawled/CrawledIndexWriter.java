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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.naumen.gp.crawled.CrawledConstants.CLEAN_TITLE;
import static ru.naumen.gp.crawled.CrawledConstants.SIGNIFICANT_CONTENT;

public class CrawledIndexWriter implements IndexWriter {

    private static final Logger LOG = LoggerFactory.getLogger("ru.naumen.gp.crawled");

    private Configuration configuration;
    private String apiUrl;
    private int batchSize;
    private String crawledScanId;

    private CloseableHttpClient http;
    private Collection<NutchDocument> forAdd;
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
        this.crawledScanId = getConfig(CrawledConstants.CRAWLED_SCAN_ID);
        this.batchSize = configuration.getInt(CrawledConstants.BATCH_SIZE, 100);
        this.forAdd = new ArrayList<>(this.batchSize);
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
        if (doc.getFieldValue(SIGNIFICANT_CONTENT) != null) {
            forAdd.add(doc);
            if (forAdd.size() >= batchSize) commit();
        }
    }

    @Override
    public void write(NutchDocument doc) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("add doc {}", doc.getField("id"));
        }

        add(doc);
    }

    @Override
    public void update(NutchDocument doc) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("update doc {}", doc.getField("id"));
        }

        add(doc);
    }

    @Override
    public void delete(String key) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("delete doc with key {}", key);
        }

        LOG.warn("Crawled does not use nutch delete functionality!");
    }

    private void save(String data) throws IOException {
        String url = apiUrl + "/save";
        HttpEntity entity = new StringEntity(data, StandardCharsets.UTF_8);

        HttpPost request = new HttpPost(url);
        request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8");
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

    private String serializeForAdd() throws IOException {
        Collection<Map<String, Object>> docs = new ArrayList<>(forAdd.size());
        for (NutchDocument doc : forAdd) {
            Map<String, Object> docMap = new HashMap<>();
            docMap.put("title", doc.getFieldValue(CLEAN_TITLE));
            docMap.put("url", doc.getFieldValue("url"));
            docMap.put("checksum", doc.getFieldValue("digest"));
            docMap.put("tstamp", doc.getFieldValue("tstamp"));
            docMap.put("content", doc.getFieldValue(SIGNIFICANT_CONTENT));
            docs.add(docMap);
        }

        Map<String, Object> batchMap = new HashMap<>();
        batchMap.put("scanId", crawledScanId);
        batchMap.put("documents", docs);

        String json = jsonMapper.writeValueAsString(batchMap);

        if (LOG.isDebugEnabled()) {
            LOG.debug(json);
        }

        return json;
    }

    @Override
    public void commit() throws IOException {
        if (!forAdd.isEmpty()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("trying to send docs to app");
            }
            save(serializeForAdd());
            forAdd.clear();
        }
    }

    @Override
    public void close() throws IOException {
        commit();
        http.close();
        LOG.info("end of indexing; close");
    }

    @Override
    public String describe() {
        return "Crawled app API writer\n" +
                "\t" + CrawledConstants.SERVER_URL + ": url of API\n" +
                "\t" + CrawledConstants.BATCH_SIZE + ": size of buffer\n";
    }

}