package org.jsoup.safety;

import org.jsoup.helper.Validate;
import org.jsoup.nodes.*;
import org.jsoup.parser.Tag;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

public class ContentCleaner {

    private final Whitelist whitelist;

    public ContentCleaner(Whitelist whitelist) {
        Validate.notNull(whitelist);

        this.whitelist = whitelist;
    }

    public Document clean(Document unclean) {
        Validate.notNull(unclean);

        Document clean = new Document(unclean.baseUri());
        NodeTraversor.traverse(new CleaningVisitor(clean), unclean);

        return clean;
    }

    private final class CleaningVisitor implements NodeVisitor {
        private Element destination;

        private CleaningVisitor(Element destination) {
            this.destination = destination;
        }

        private Element cleanCopy(Element sourceEl) {
            String sourceTag = sourceEl.tagName();

            Attributes destAttrs = new Attributes();
            for (Attribute attr : sourceEl.attributes()) {
                if (whitelist.isSafeAttribute(sourceTag, sourceEl, attr)) {
                    destAttrs.put(attr);
                }
            }
            Attributes enforcedAttrs = whitelist.getEnforcedAttributes(sourceTag);
            destAttrs.addAll(enforcedAttrs);

            return new Element(Tag.valueOf(sourceTag), sourceEl.baseUri(), destAttrs);
        }

        public void head(Node source, int depth) {
            if (source instanceof Element) {
                Element sourceEl = (Element) source;
                if (whitelist.isSafeTag(sourceEl.tagName())) {
                    Element destChild = cleanCopy(sourceEl);
                    destination.appendChild(destChild);
                    destination = destChild;
                }
            } else if (source instanceof TextNode) {
                TextNode sourceText = (TextNode) source;
                TextNode destText = new TextNode(sourceText.getWholeText());
                destination.appendChild(destText);
            } else if (source instanceof DataNode && whitelist.isSafeTag(source.parent().nodeName())) {
                DataNode sourceData = (DataNode) source;
                DataNode destData = new DataNode(sourceData.getWholeData());
                destination.appendChild(destData);
            }
        }

        public void tail(Node source, int depth) {
            if (source instanceof Element && whitelist.isSafeTag(source.nodeName())) {
                destination = destination.parent();
            }

        }
    }

}
