package ru.naumen.gp.crawled;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.*;
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
import java.util.*;

import static ru.naumen.gp.crawled.CrawledConstants.BLACKLIST_AREAS_SELECTOR;
import static ru.naumen.gp.crawled.CrawledConstants.CONTENT_SELECTOR;
import static ru.naumen.gp.crawled.CrawledConstants.SIGNIFICANT_CONTENT;

public class CrawledParseFilter implements HtmlParseFilter {

    private Configuration cfg;
    private String significantContentSelector;
    private String blacklistAreasSelector;

    private static final Logger LOG = LoggerFactory.getLogger("ru.naumen.gp.crawled");
    private static final ContentCleaner CLEANER = new ContentCleaner(Whitelist.relaxed());

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        Document jsoupDoc = parseDocument(content);
        Parse parse = parseResult.get(content.getUrl());

        if (significantContentSelector != null) {
            Document selected = selectElements(jsoupDoc);

            if (LOG.isTraceEnabled()) {
                LOG.trace(selected.html());
            }

            String significantContent = CLEANER.clean(selected).html();

            if (LOG.isTraceEnabled()) {
                LOG.trace(significantContent);
            }

            parse.getData().getContentMeta().set(SIGNIFICANT_CONTENT, significantContent);
        }

        if (blacklistAreasSelector != null) {
            Outlink[] inputOutlinks = parse.getData().getOutlinks();
            Outlink[] outputOutlinks = filterOutlinks(jsoupDoc, inputOutlinks);
            parse.getData().setOutlinks(outputOutlinks);
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
        Document wrapper = new Document(doc.baseUri());
        for (Element matched : doc.select(significantContentSelector)) {
            wrapper.appendChild(matched);
        }
        return wrapper;
    }

    private Outlink[] filterOutlinks(Document doc, Outlink[] parsedOutlinks) {
        Outlink[] result;

        if (parsedOutlinks.length > 0) {
            //remove blacklisted areas
            doc.body().select(blacklistAreasSelector).remove();

            //collect links from remaining
            Elements nonBlacklisted = doc.body().select("a[href], area[href]");
            Set<String> nonBlacklistedLinks = new HashSet<>(nonBlacklisted.size());
            for (Element linkElem : nonBlacklisted) {
                String attr = linkElem.absUrl("href");
                if (attr != null) {
                    nonBlacklistedLinks.add(attr);
                }
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("notBlacklistedLinks: " + nonBlacklistedLinks.toString());
            }

            //filter out previously collected outlinks from whole page
            ArrayList<Outlink> filtered = new ArrayList<>(Math.min(nonBlacklistedLinks.size(), parsedOutlinks.length));
            for (Outlink o: parsedOutlinks) {
                if (nonBlacklistedLinks.contains(o.getToUrl())) {
                    filtered.add(o);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("use outlink " + o.getToUrl());
                    }
                } else if (LOG.isTraceEnabled()) {
                    LOG.trace("skip outlink " + o.getToUrl());
                }
            }

            result = filtered.toArray(new Outlink[filtered.size()]);
        } else {
            result = parsedOutlinks;
        }

        return result;
    }

    @Override
    public void setConf(Configuration configuration) {
        cfg = configuration;
        significantContentSelector = fromConfig(CONTENT_SELECTOR);
        blacklistAreasSelector = fromConfig(BLACKLIST_AREAS_SELECTOR);
    }

    @Override
    public Configuration getConf() {
        return cfg;
    }

    private String fromConfig(String key) {
        String value = getConf().get(key);
        return value != null && value.trim().length() > 0 ? value : null;
    }

}