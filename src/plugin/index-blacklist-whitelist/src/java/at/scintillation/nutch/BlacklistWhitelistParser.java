package at.scintillation.nutch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to parse the content and apply a blacklist or whitelist. The content is stored in 
 * the index in the field "strippedContent".<br/>
 * If a blacklist configuration is provided, all elements plus their subelements are not included in the
 * final content field which is indexed. If a whitelist configuration is provided, only the elements
 * and their subelements are included in the indexed field.<br/><br/>
 * On the basis of {@link https://issues.apache.org/jira/browse/NUTCH-585}
 * 
 * @author Elisabeth Adler
 */
public class BlacklistWhitelistParser implements HtmlParseFilter {

    private static final Log LOG = LogFactory.getLog("at.scintillation.nutch");
    private static final TransformerFactory transformers = TransformerFactory.newInstance();

    private Configuration conf;

    private String[] blacklist;
    private String[] whitelist;

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        try {

            Parse parse = parseResult.get(content.getUrl());

            DocumentFragment rootToIndex = null;
            if ((this.whitelist != null) && (this.whitelist.length > 0)) {
                LOG.info("Applying whitelist...");
                rootToIndex = (DocumentFragment) doc.cloneNode(false);
                whitelisting(doc, rootToIndex);
            } else if ((this.blacklist != null) && (this.blacklist.length > 0)) {
                LOG.info("Applying blacklist...");
                rootToIndex = (DocumentFragment) doc.cloneNode(true);
                blacklisting(rootToIndex);
            }

            if (rootToIndex != null) {
                cleanSelected(rootToIndex); // extract text to index

                StringWriter sw = new StringWriter();
                try {
                    Transformer t = transformers.newTransformer();
                    t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                    t.setOutputProperty(OutputKeys.METHOD, "html");
                    t.transform(new DOMSource(rootToIndex), new StreamResult(sw));
                } catch (TransformerException e) {
                    sw.write("");
                }

                LOG.warn(sw.toString());
                parse.getData().getContentMeta().set("strippedContent", sw.toString());
            }

        } catch (Exception e) {
            LOG.error("error", e);
            throw e;
        }

        return parseResult;
    }

    /**
     * Traverse through the document and set all elements matching the given
     * blacklist configuration to empty
     * @param pNode Root node
     */
    private void blacklisting(Node pNode) {
        if (inList(false, pNode)) {
            // can't remove this node, but we can strip it
            pNode.setNodeValue("");
            // remove all children for this node
            while (pNode.hasChildNodes())
                pNode.removeChild(pNode.getFirstChild());
        } else {
            // process the children recursively
            NodeList children = pNode.getChildNodes();
            if (children != null) {
                int len = children.getLength();
                for (int i = 0; i < len; i++) {
                    blacklisting(children.item(i));
                }
            }
        }
    }

    /**
     * Traverse through the document and copy all elements matching the given
     * whitelist configuration to the new node parameter, which will then only
     * contain all allowed nodes including all their children.
     * @param pNode Root node
     * @param newNode node containing only the allowed elements
     */
    private void whitelisting(Node pNode, Node newNode) {
        if (inList(true, pNode)) {
            // append listed node to result
            newNode.appendChild(pNode.cloneNode(true));
        } else {
            // process the children recursively
            NodeList children = pNode.getChildNodes();
            if (children != null) {
                int len = children.getLength();
                for (int i = 0; i < len; i++) {
                    whitelisting(children.item(i), newNode);
                }
            }
        }
    }

    private boolean inList(boolean whitelist, Node node) {
        String[] list = whitelist ? this.whitelist : this.blacklist;
        String type = node.getNodeName().toLowerCase();
        String typeAndId = null;
        String typeAndClass = null;
        if (node.hasAttributes()) {
            Node attr = node.getAttributes().getNamedItem("id");
            typeAndId = (attr != null) ? type + "#" + attr.getNodeValue().toLowerCase() : null;

            attr = node.getAttributes().getNamedItem("class");
            typeAndClass = (attr != null) ? type + "." + attr.getNodeValue().toLowerCase() : null;
        }

        // check if the given element is in white- or black- list: either only the element type, or type and id or type and class
        boolean inList = Arrays.binarySearch(list, type) >= 0 ||
                (typeAndId != null && Arrays.binarySearch(list, typeAndId) >= 0) ||
                (typeAndClass != null && Arrays.binarySearch(list, typeAndClass) >= 0);

        if (LOG.isTraceEnabled()) {
            String listType = (whitelist ? "whitelist" : "blacklist");
            String nodeIs = (typeAndId != null ? typeAndId : (typeAndClass != null ? typeAndClass : type));
            LOG.trace(String.format("In %s: %b (is %s)", listType, inList, nodeIs));
        }

        return inList;
    }

    /**
     * copied from {@link org.apache.nutch.parse.html.DOMContentUtils}
     */
    private void cleanSelected(Node node) {
        List<Node> removingList = new ArrayList<>();

        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            LOG.warn(child.getNodeType() + " " + child.getNodeName());

            if (child.getNodeType() == Node.COMMENT_NODE) {
                removingList.add(child);
            } else if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (nodeHasName(child, "style") || nodeHasName(child, "script")) {
                    removingList.add(child);
                } else {
                    cleanSelected(child);
                }
            }
        }

        if (node.hasAttributes()) {
            removeAttributeIfExists(node, "id");
            removeAttributeIfExists(node, "class");
            removeAttributeIfExists(node, "style");
        }

        for (Node removing: removingList) {
            node.removeChild(removing);
        }
    }

    private boolean nodeHasName(Node node, String name) {
        return name.equals(node.getNodeName());
    }

    private void removeAttributeIfExists(Node node, String name) {
        NamedNodeMap attributes = node.getAttributes();
        if (attributes.getNamedItem(name) != null) attributes.removeNamedItem(name);
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        // parse configuration for blacklist
        this.blacklist = null;
        String elementsToExclude = getConf().get("parser.html.blacklist", null);
        if ((elementsToExclude != null) && (elementsToExclude.trim().length() > 0)) {
            elementsToExclude = elementsToExclude.toLowerCase(); // convert to lower case so that there's no case problems
            LOG.info("Configured using [parser.html.blacklist] to ignore elements [" + elementsToExclude + "]...");
            this.blacklist = elementsToExclude.split(",");
            Arrays.sort(this.blacklist); // required for binary search
        }

        // parse configuration for whitelist
        this.whitelist = null;
        String elementsToInclude = getConf().get("parser.html.whitelist", null);
        if ((elementsToInclude != null) && (elementsToInclude.trim().length() > 0)) {
            elementsToInclude = elementsToInclude.toLowerCase(); // convert to lower case so that there's no case problems
            LOG.info("Configured using [parser.html.whitelist] to only use elements [" + elementsToInclude + "]...");
            this.whitelist = elementsToInclude.split(",");
            Arrays.sort(this.whitelist); // required for binary search
        }
    }

    public Configuration getConf() {
        return this.conf;
    }

}
