package ru.naumen.gp.crawled;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.ContentCleaner;
import org.jsoup.safety.Whitelist;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static ru.naumen.gp.crawled.CrawledConstants.CONTENT_SELECTORS;
import static ru.naumen.gp.crawled.CrawledConstants.SIGNIFICANT_CONTENT;

public class CrawledParseFilter implements HtmlParseFilter {

    private Configuration cfg;
    private String selector;

    private static final Logger LOG = LoggerFactory.getLogger("ru.naumen.gp.crawled");
    private static final ContentCleaner CLEANER = new ContentCleaner(Whitelist.relaxed());

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        if (selector != null) {
            Document selected = selectElements(parseDocument(content));

            if (LOG.isDebugEnabled()) {
                LOG.debug(selected.html());
            }

            String significantContent = CLEANER.clean(selected).html();

            if (LOG.isDebugEnabled()) {
                LOG.debug(significantContent);
            }

            parseResult
                    .get(content.getUrl())
                    .getData()
                    .getContentMeta()
                    .set(SIGNIFICANT_CONTENT, significantContent);
        }

        return parseResult;
    }

    private Document parseDocument(Content content) {
        try {
            return Jsoup.parse(new ByteArrayInputStream(content.getContent()), null, content.getBaseUrl());
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Document selectElements(Document doc) {
        Elements matched = doc.select(selector);
        Document wrapper = new Document(doc.baseUri());
        for (Element e : matched) {
            wrapper.appendChild(e);
        }
        return wrapper;
    }

    @Override
    public void setConf(Configuration configuration) {
        cfg = configuration;
        selector = getConf().get(CONTENT_SELECTORS);
    }

    @Override
    public Configuration getConf() {
        return cfg;
    }

}