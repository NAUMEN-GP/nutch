/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.selenium;

// JDK imports

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpException;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;

/* Most of this code was borrowed from protocol-htmlunit; which in turn borrowed it from protocol-httpclient */

public class HttpResponse implements Response {

  private Http http;
  private URL url;
  private String orig;
  private String base;
  private byte[] content;
  private int code;
  private Metadata headers = new SpellCheckedMetadata();

  /** The nutch configuration */
  private Configuration conf = null;

  protected enum Scheme {
    HTTP, HTTPS,
  }

  public HttpResponse(Http http, URL url, CrawlDatum datum) throws ProtocolException, IOException {

    this.conf = http.getConf();
    this.http = http;
    this.url = url;
    this.orig = url.toString();
    this.base = url.toString();

    Scheme scheme;
    if ("http".equals(url.getProtocol())) {
      scheme = Scheme.HTTP;
    } else if ("https".equals(url.getProtocol())) {
      scheme = Scheme.HTTPS;
    } else {
      throw new HttpException("Unknown scheme (not http/https) for url:" + url);
    }

    if (Http.LOG.isTraceEnabled()) {
      Http.LOG.trace("fetching " + url);
    }

    String path = "".equals(url.getFile()) ? "/" : url.getFile();

    // some servers will redirect a request with a host line like
    // "Host: <hostname>:80" to "http://<hpstname>/<orig_path>"- they
    // don't want the :80...

    String host = url.getHost();
    int port;
    String portString;
    if (url.getPort() == -1) {
      port = scheme == Scheme.HTTP ? 80 : 443;
      portString = "";
    } else {
      port = url.getPort();
      portString = ":" + port;
    }
    Socket socket = null;

    try {
      socket = new Socket(); // create the socket
      socket.setSoTimeout(http.getTimeout());

      // connect
      String sockHost = http.useProxy(url) ? http.getProxyHost() : host;
      int sockPort = http.useProxy(url) ? http.getProxyPort() : port;
      InetSocketAddress sockAddr = new InetSocketAddress(sockHost, sockPort);
      socket.connect(sockAddr, http.getTimeout());

      // make socket SSL'ed (if https)
      if (scheme == Scheme.HTTPS) {
        SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket sslsocket = (SSLSocket) factory.createSocket(socket, sockHost, sockPort, true);
        sslsocket.setUseClientMode(true);
        sslsocket.startHandshake();
        socket = sslsocket;
      }

      // make request
      OutputStream req = socket.getOutputStream();

      StringBuilder reqStr = new StringBuilder("GET ");
      if (http.useProxy(url)) {
        reqStr.append(url.getProtocol()).append("://").append(host).append(portString).append(path);
      } else {
        reqStr.append(path);
      }

      reqStr.append(" HTTP/1.0\r\n");

      reqStr.append("Host: ").append(host).append(portString).append("\r\n");

      reqStr.append("Accept-Encoding: x-gzip, gzip, deflate\r\n");

      String userAgent = http.getUserAgent();
      if ((userAgent == null) || (userAgent.length() == 0)) {
        if (Http.LOG.isErrorEnabled()) {
          Http.LOG.error("User-agent is not set!");
        }
      } else {
        reqStr.append("User-Agent: ").append(userAgent).append("\r\n");
      }

      reqStr.append("Accept-Language: ").append(this.http.getAcceptLanguage()).append("\r\n");

      reqStr.append("Accept: ").append(this.http.getAccept()).append("\r\n");

      if (datum.getModifiedTime() > 0) {
        reqStr.append("If-Modified-Since: ").append(HttpDateFormat.toString(datum.getModifiedTime())).append("\r\n");
      }

      reqStr.append("\r\n");

      byte[] reqBytes = reqStr.toString().getBytes();

      req.write(reqBytes);
      req.flush();

      PushbackInputStream in = // process response
          new PushbackInputStream(new BufferedInputStream(socket.getInputStream(), Http.BUFFER_SIZE), Http.BUFFER_SIZE);

      StringBuilder line = new StringBuilder();

      boolean haveSeenNonContinueStatus = false;
      while (!haveSeenNonContinueStatus) {
        // parse status code line
        this.code = parseStatusLine(in, line);
        // parse headers
        parseHeaders(in, line);
        haveSeenNonContinueStatus = code != 100; // 100 is "Continue"
      }

      // Get Content type header
      String contentType = getHeader(Response.CONTENT_TYPE);

      // handle with Selenium only if content type in HTML or XHTML 
      if (contentType != null && (contentType.contains("text/html") || contentType.contains("application/xhtml"))) {
        readWithSelenium(url);
      } else {
        readRaw(in);
      }

    } finally {
      if (socket != null)
        socket.close();
    }
  }

  /* ------------------------- *
   * <implementation:Response> *
   * ------------------------- */

  public URL getUrl() {
    return url;
  }

  public int getCode() {
    return code;
  }

  public String getHeader(String name) {
    return headers.get(name);
  }

  public Metadata getHeaders() {
    return headers;
  }

  public byte[] getContent() {
    return content;
  }

  /* ------------------------- *
   * <implementation:Response> *
   * ------------------------- */

  private void readWithSelenium(URL url) throws IOException {
    String page = HttpWebClient.getHtmlPage(url.toString(), conf);
    content = page.getBytes("UTF-8");
  }

  private void readRaw(InputStream in) throws HttpException, IOException {
    int contentLength = Integer.MAX_VALUE; // get content length
    String contentLengthString = headers.get(Response.CONTENT_LENGTH);
    if (contentLengthString != null) {
      contentLengthString = contentLengthString.trim();
      try {
        if (!contentLengthString.isEmpty())
          contentLength = Integer.parseInt(contentLengthString);
      } catch (NumberFormatException e) {
        throw new HttpException("bad content length: " + contentLengthString);
      }
    }
    if (http.getMaxContent() >= 0 && contentLength > http.getMaxContent()) {// limit download size
      contentLength = http.getMaxContent();
    }

    // do not try to read if the contentLength is 0
    if (contentLength == 0) {
      content = new byte[0];
      return;
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream(Http.BUFFER_SIZE);
    byte[] bytes = new byte[Http.BUFFER_SIZE];
    int length = 0;

    // read content
    int i = in.read(bytes);
    while (i != -1) {
      out.write(bytes, 0, i);
      length += i;
      if (length >= contentLength) {
        break;
      }
      if ((length + Http.BUFFER_SIZE) > contentLength) {
        // reading next chunk may hit contentLength,
        // must limit number of bytes read
        i = in.read(bytes, 0, (contentLength - length));
      } else {
        i = in.read(bytes);
      }
    }

    content = out.toByteArray();
  }

  private int parseStatusLine(PushbackInputStream in, StringBuilder line) throws IOException, HttpException {
    readLine(in, line, false);

    int codeStart = line.indexOf(" ");
    int codeEnd = line.indexOf(" ", codeStart + 1);

    // handle lines with no plaintext result code, ie:
    // "HTTP/1.1 200" vs "HTTP/1.1 200 OK"
    if (codeEnd == -1)
      codeEnd = line.length();

    int code;
    try {
      code = Integer.parseInt(line.substring(codeStart + 1, codeEnd));
    } catch (NumberFormatException e) {
      throw new HttpException("bad status line '" + line + "': " + e.getMessage(), e);
    }

    return code;
  }

  private void processHeaderLine(StringBuilder line) throws IOException, HttpException {

    int colonIndex = line.indexOf(":"); // key is up to colon
    if (colonIndex == -1) {
      int i;
      for (i = 0; i < line.length(); i++)
        if (!Character.isWhitespace(line.charAt(i)))
          break;
      if (i == line.length())
        return;
      throw new HttpException("No colon in header:" + line);
    }
    String key = line.substring(0, colonIndex);

    int valueStart = colonIndex + 1; // skip whitespace
    while (valueStart < line.length()) {
      int c = line.charAt(valueStart);
      if (c != ' ' && c != '\t')
        break;
      valueStart++;
    }
    String value = line.substring(valueStart);
    headers.set(key, value);
  }

  // Adds headers to our headers Metadata
  private void parseHeaders(PushbackInputStream in, StringBuilder line) throws IOException, HttpException {

    while (readLine(in, line, true) != 0) {

      // handle HTTP responses with missing blank line after headers
      int pos;
      if (((pos = line.indexOf("<!DOCTYPE")) != -1) || ((pos = line.indexOf("<HTML")) != -1)
          || ((pos = line.indexOf("<html")) != -1)) {

        in.unread(line.substring(pos).getBytes("UTF-8"));
        line.setLength(pos);

        try {
          //TODO: (CM) We don't know the header names here
          //since we're just handling them generically. It would
          //be nice to provide some sort of mapping function here
          //for the returned header names to the standard metadata
          //names in the ParseData class
          processHeaderLine(line);
        } catch (Exception e) {
          // fixme:
          Http.LOG.warn("Error: ", e);
        }
        return;
      }

      processHeaderLine(line);
    }
  }

  private static int readLine(PushbackInputStream in, StringBuilder line, boolean allowContinuedLine)
      throws IOException {
    line.setLength(0);

    int c;
    while ((c = in.read()) != -1) {
      if (c == '\r') {
        skip(in, '\n');
        c = '\n';
      }

      if (c == '\n') {
        if (line.length() > 0 && allowContinuedLine && skip(in, ' ', '\t')) {
          c = ' ';
        } else {
          return line.length();
        }
      }

      line.append((char) c);
    }

    throw new EOFException();
  }

  private static boolean skip(PushbackInputStream in, char... chars) throws IOException {
    for (char ch: chars) {
      if (peek(in) == ch) {
        in.read();
        return true;
      }
    }

    return false;
  }

  private static int peek(PushbackInputStream in) throws IOException {
    int value = in.read();
    in.unread(value);
    return value;
  }
}
