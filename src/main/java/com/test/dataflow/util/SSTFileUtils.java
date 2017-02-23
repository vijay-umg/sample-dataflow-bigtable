package com.test.dataflow.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;

/**
 * Created by arumugv on 2/22/17.
 */
public class SSTFileUtils {

    public static String loadFilefromClasspath(final String resourceName) throws IOException {
        String content = null;
        try (final InputStream ipStream =
                     SSTFileUtils.class.getClassLoader().getResourceAsStream(resourceName)) {
            content = IOUtils.toString(ipStream);
        }

        return content;
    }
}
