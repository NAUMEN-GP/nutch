package ru.naumen.gp.crawled;

public interface CrawledConstants {
    String CRAWLED_PREFIX = "crawled.";

    String SERVER_URL = CRAWLED_PREFIX + "url";
    String BATCH_SIZE = CRAWLED_PREFIX + "batchSize";
    String CRAWLED_SCAN_ID = CRAWLED_PREFIX + "scanId";

    String CONTENT_SELECTORS = CRAWLED_PREFIX + "content.selectors";
    String CONTENT_MIN_LENGTH = CRAWLED_PREFIX + "content.minLength";

    String SIGNIFICANT_CONTENT = "significantContent";
}