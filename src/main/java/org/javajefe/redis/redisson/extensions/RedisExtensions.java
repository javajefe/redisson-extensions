package org.javajefe.redis.redisson.extensions;

import com.google.gson.Gson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by BukarevAA on 18.02.2019.
 */
public class RedisExtensions {

    private static final Logger log = LoggerFactory.getLogger(RedisExtensions.class);
    private enum SCRIPTS {

        batchXADD("/lua/batchXADD.lua");

        private String path;

        SCRIPTS(String path) {
            this.path = path;
        }

        String getPath() {
            return this.path;
        }
    }
    private final Gson gson;
    private final RedissonClient redissonClient;
    private final RScript rScript;
    private final Map<SCRIPTS, String> loadedScriptIds;

    public RedisExtensions(RedissonClient redissonClient) throws IOException {
        this.gson = new Gson();
        this.redissonClient = redissonClient;
        this.rScript = redissonClient.getScript(StringCodec.INSTANCE);
        Map<SCRIPTS, String> scripts = new HashMap<>();
        for (SCRIPTS scriptMeta: SCRIPTS.values()) {
            String lua = Utils.loadResource(scriptMeta.getPath());
            String sha = rScript.scriptLoad(lua);
            log.debug("Loaded script {} with sha marker {}", sha);
            scripts.put(scriptMeta, sha);
        }
        this.loadedScriptIds = Collections.unmodifiableMap(scripts);
    }

    public void batchXADD(String key, List<Map<String, String>> messages) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is required");
        }
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages are required");
        }
        if (messages.stream().flatMap(m -> m.values().stream()).anyMatch(v -> v == null)) {
            throw new IllegalArgumentException("null values are disallowed");
        }
        String sha = loadedScriptIds.get(SCRIPTS.batchXADD);
        Object[] argv = messages.stream()
                .map(m -> gson.toJson(m))
                .toArray();
        rScript.evalSha(RScript.Mode.READ_ONLY, sha, RScript.ReturnType.VALUE,
                Collections.singletonList(key), argv);
    }
}
