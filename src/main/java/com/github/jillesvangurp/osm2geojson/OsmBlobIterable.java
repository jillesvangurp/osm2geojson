/**
 * Copyright (c) 2013, Jilles van Gurp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.github.jillesvangurp.osm2geojson;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.jillesvangurp.iterables.LineIterable;

/**
 * Iterates over the open street map xml and yields one parseable node, way, or relation xml string blob each time next
 * is called. Using this class, you can use a simple for loop to loop over the xml.
 */
public final class OsmBlobIterable implements Iterable<String> {

    private final LineIterable lineIterable;

    public OsmBlobIterable(LineIterable lineIterable) {
        this.lineIterable = lineIterable;
    }

    @Override
    public Iterator<String> iterator() {

        final Iterator<String> it = lineIterable.iterator();
        return new Iterator<String>() {
            String next = null;
            StringBuilder buf = new StringBuilder();

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                } else {
                    String line;
                    while (it.hasNext() && next == null) {
                        line = it.next();
                        if (line.length() > 0) {
                            if (line.trim().startsWith("<node")) {
                                buf.delete(0, buf.length());
                                buf.append(line);
                                if (!fastEndsWith(line, "/>")) {
                                    while (!fastEndsWith(line.trim(), "</node>")) {
                                        line = it.next();
                                        buf.append(line);
                                    }
                                }
                                next = buf.toString().trim();
                            } else if (line.trim().startsWith("<way")) {
                                buf.delete(0, buf.length());
                                buf.append(line);
                                if (!fastEndsWith(line, "/>")) {
                                    while (!fastEndsWith(line.trim(), "</way>")) {
                                        line = it.next();
                                        buf.append(line);
                                    }
                                }
                                next = buf.toString().trim();
                            } else if (line.trim().startsWith("<relation")) {
                                buf.delete(0, buf.length());
                                buf.append(line);
                                if (!fastEndsWith(line, "/>")) {
                                    while (!fastEndsWith(line.trim(), "</relation>")) {
                                        line = it.next();
                                        buf.append(line);
                                    }
                                }
                                next = buf.toString().trim();
                            }
                        }
                    }
                    return next != null;
                }
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String result = next;
                    next = null;
                    return result;
                } else {
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove is not supported");
            }
        };
    }

    static boolean fastEndsWith(CharSequence buf, String postFix) {
        // String.endsWith is very slow and creating extra String objects
        // every time we want to check the CharSequence content is
        // inefficient. This implementation simply inspects the end of the
        // CharSequence one character at the time.
        if (buf.length() < postFix.length()) {
            return false;
        } else {
            boolean match = true;
            for (int i = 1; i <= postFix.length(); i++) {
                match = match && buf.charAt(buf.length() - i) == postFix.charAt(postFix.length() - i);
                if (!match) {
                    return false;
                }
            }
            return match;
        }
    }
}