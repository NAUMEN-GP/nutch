package ru.naumen.gp.crawled;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static ru.naumen.gp.crawled.CrawledConstants.CLEAN_TITLE;
import static ru.naumen.gp.crawled.CrawledConstants.CONTENT_MIN_LENGTH;
import static ru.naumen.gp.crawled.CrawledConstants.SIGNIFICANT_CONTENT;

public class CrawledIndexingFilter implements IndexingFilter {

    private Configuration cfg;
    private int contentMinLength;
    private static final Logger LOG = LoggerFactory.getLogger("ru.naumen.gp.crawled");

    @Override
    public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) throws IndexingException {
        ParseData parseData = parse.getData();
        String significantContent = parseData.getMeta(SIGNIFICANT_CONTENT);

        if (significantContent != null && significantContent.trim().length() >= contentMinLength) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("content ok");
            }

            doc.add(SIGNIFICANT_CONTENT, significantContent);
            doc.add(CLEAN_TITLE, parseData.getMeta(CLEAN_TITLE));
            return doc;
        } else {
            if (LOG.isTraceEnabled()) {
                LOG.trace("content skipped");
            }

            return null;
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        cfg = configuration;
        contentMinLength = getConf().getInt(CONTENT_MIN_LENGTH, 300);
    }

    @Override
    public Configuration getConf() {
        return cfg;
    }

}