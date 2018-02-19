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

import java.util.*;

import static ru.naumen.gp.crawled.CrawledConstants.*;

public class CrawledParseFilter implements HtmlParseFilter {

    private Configuration cfg;
    private String significantContentSelector;
    private String blacklistAreasSelector;

    private static final Logger LOG = LoggerFactory.getLogger("ru.naumen.gp.crawled");
    private static final ContentCleaner CLEANER = new ContentCleaner(Whitelist.relaxed());

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        Document jsoupDoc = parseDocument(content);
        ParseData parseData = parseResult.get(content.getUrl()).getData();

        String title = jsoupDoc.head().selectFirst("title").text();
        parseData.getContentMeta().set(CLEAN_TITLE, title);

        if (significantContentSelector != null) {
            String significantContent = selectSignificantContent(jsoupDoc);
            parseData.getContentMeta().set(SIGNIFICANT_CONTENT, significantContent);
        }

        if (blacklistAreasSelector != null) {
            Outlink[] filteredOutlinks = filterOutlinks(jsoupDoc, parseData.getOutlinks());
            parseData.setOutlinks(filteredOutlinks);
        }

        return parseResult;
    }

    private Document parseDocument(Content content) {
        String docStr = new String(content.getContent());
        return Jsoup.parse(docStr, content.getBaseUrl());
    }

    private String selectSignificantContent(Document doc) {
        Elements selected = doc.select(significantContentSelector);

        if (LOG.isTraceEnabled()) {
            LOG.trace("selectedAsSignificant: " + selected.size());
        }

        Document selectionWrapper = new Document(doc.baseUri());
        for (Element matched : selected) {
            selectionWrapper.appendChild(matched.clone());
        }

        return CLEANER.clean(selectionWrapper).html();
    }

    private Outlink[] filterOutlinks(Document doc, Outlink[] parsedOutlinks) {
        Outlink[] result;

        if (parsedOutlinks.length > 0) {
            Element bodyClone = doc.body().clone();
            //remove blacklisted areas
            bodyClone.select(blacklistAreasSelector).remove();

            //collect links from remaining
            Elements nonBlacklisted = bodyClone.select("a[href], area[href]");

            if (LOG.isTraceEnabled()) {
                LOG.trace("notBlacklistedElems: " + nonBlacklisted.size());
            }

            Set<String> nonBlacklistedLinks = new HashSet<>(nonBlacklisted.size());
            for (Element linkElem : nonBlacklisted) {
                String href = linkElem.absUrl("href").trim();
                if (!"".equals(href)) {
                    nonBlacklistedLinks.add(href);
                }
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("notBlacklistedLinks: " + nonBlacklistedLinks.toString());
            }

            //filter out previously collected outlinks from whole page
            ArrayList<Outlink> filtered = new ArrayList<>(Math.min(nonBlacklistedLinks.size(), parsedOutlinks.length));
            for (Outlink o: parsedOutlinks) {
                if (nonBlacklistedLinks.contains(o.getToUrl().trim())) {
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Approved outlinks: " + result.length + " from " + parsedOutlinks.length);
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