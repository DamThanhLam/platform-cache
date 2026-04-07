package org.sento.platform.cache.util;

public class CacheKeyBuilder {

    public static String build(String prefix, String... parts) {
        StringBuilder sb = new StringBuilder(prefix);
        for (String part : parts) {
            sb.append(":").append(part);
        }
        return sb.toString();
    }
}
