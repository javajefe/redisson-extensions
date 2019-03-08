package tech.javajefe.redis.redisson.extensions;

import com.google.gson.Gson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by BukarevAA on 18.02.2019.
 */
public class RedisExtensions {

    private static final Logger log = LoggerFactory.getLogger(RedisExtensions.class);

    private final Gson gson;
    private final RScript rScript;
    private final Map<Scripts, String> loadedScriptIds;

    public RedisExtensions(RedissonClient redissonClient) throws IOException {
        this.gson = new Gson();
        this.rScript = redissonClient.getScript(StringCodec.INSTANCE);
        Map<Scripts, String> scripts = new ConcurrentHashMap<>();
        for (Scripts scriptMeta: Scripts.values()) {
            scripts.put(scriptMeta, loadScript(scriptMeta));
        }
        this.loadedScriptIds = scripts;
    }

    private String loadScript(Scripts scriptMeta) throws IOException {
        String lua = Utils.loadResource(scriptMeta.getPath());
        String sha = rScript.scriptLoad(lua);
        log.debug("Loaded script {} with sha marker {}", scriptMeta.getPath(), sha);
        return sha;
    }

    private <R> R evalShaWithRetry(Scripts script, List<Object> keys, Object...args) {
        return evalShaWithRetry(script, keys, true, args);
    }

    private <R> R evalShaWithRetry(Scripts script, List<Object> keys, boolean firstAttempt, Object...args) {
        R result = null;
        String sha = loadedScriptIds.get(script);
        try {
            result = rScript.evalSha(script.getMode(), sha, script.getReturnType(), keys, args);
        } catch (RedisException ex) {
            if (!firstAttempt || ex.getMessage() == null || !ex.getMessage().startsWith("NOSCRIPT")) {
                throw ex;
            }
            log.warn("No script {} with sha marker {}. Trying to reload.", script.getPath(), sha);
            try {
                String newSha = loadScript(script);
                loadedScriptIds.put(script, newSha);
                result = evalShaWithRetry(script, keys, false, args);
            } catch (IOException ioex) {
                throw ex;
            }
        }
        return result;
    }

    public List<StreamMessageId> batchXADD(String key, List<Map<String, String>> messages) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is required");
        }
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages are required");
        }
        if (messages.stream().flatMap(m -> m.values().stream()).anyMatch(v -> v == null)) {
            throw new IllegalArgumentException("null values are disallowed");
        }
        Object[] argv = messages.stream()
                .map(m -> gson.toJson(m))
                .toArray();
        List<String> ids = evalShaWithRetry(Scripts.batchXADD, Collections.singletonList(key), argv);
        return ids.stream()
                .map(id -> {
                    String[] idp = id.split("-");
                    return new StreamMessageId(Long.parseLong(idp[0]), Long.parseLong(idp[1]));
                }).collect(Collectors.toList());
    }

    public Map<String, Object> XINFO_GROUPS(String key, String readGroupName) {
        List<Object> result = evalShaWithRetry(Scripts.XINFO_GROUPS, Collections.singletonList(key), readGroupName);
        Map<String, Object> info = null;
        for (Object el: result) {
            List l = (List) el;
            info = new HashMap<>();
            for (int i = 0; i < l.size(); i += 2) {
                String k = (String) l.get(i);
                Object v = l.get(i + 1);
                if ("name".equals(k) && !readGroupName.equals(v)) {
                    break;
                }
                info.put(k, v);
            }
            if (info.containsKey("name")) {
                return info;
            }
        }
        throw new IllegalArgumentException("No such reading group");
    }

    public StreamMessageId getStreamLastDeliveredId(String key, String readGroupName) {
        Map<String, Object> info = XINFO_GROUPS(key, readGroupName);
        String lastDeliveredId = (String) info.get("last-delivered-id");
        String[] ldi = lastDeliveredId.split("-");
        return new StreamMessageId(Long.parseLong(ldi[0]), Long.parseLong(ldi[1]));
    }

    public long getStreamTailSize(String key, String readGroupName) {
        StreamMessageId lastDeliveredId = getStreamLastDeliveredId(key, readGroupName);
        String pseudoNextIdToDeliver = Long.toString(lastDeliveredId.getId0()) + "-" + Long.toString(lastDeliveredId.getId1() + 1);
        return evalShaWithRetry(Scripts.getStreamTailSize, Collections.singletonList(key), pseudoNextIdToDeliver);
    }
}
