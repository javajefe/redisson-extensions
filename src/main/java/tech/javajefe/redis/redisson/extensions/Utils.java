package tech.javajefe.redis.redisson.extensions;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Created by BukarevAA on 18.02.2019.
 */
class Utils {

    public static String readFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder result = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                result.append(line).append("\n");
            }
        }
        return result.toString();
    }

    public static String loadResource(String path) throws IOException {
        Class clazz = Utils.class;
        InputStream inputStream = clazz.getResourceAsStream(path);
        if (inputStream == null) {
            throw new FileNotFoundException(path);
        }
        return readFromInputStream(inputStream);
    }
}
