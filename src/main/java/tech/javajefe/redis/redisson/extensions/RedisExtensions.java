package tech.javajefe.redis.redisson.extensions;

import com.google.gson.Gson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by BukarevAA on 18.02.2019.
 */
public class RedisExtensions {

    private static final Logger log = LoggerFactory.getLogger(RedisExtensions.class);
    private enum SCRIPTS {

        batchXADD("/lua/batchXADD.lua"),
        XINFO_GROUPS("/lua/XINFO_GROUPS.lua"),
        getStreamTailSize("/lua/getStreamTailSize.lua");

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
        String sha = loadedScriptIds.get(SCRIPTS.batchXADD);
        Object[] argv = messages.stream()
                .map(m -> gson.toJson(m))
                .toArray();
        List<String> ids = (List<String>) rScript.evalSha(RScript.Mode.READ_WRITE, sha, RScript.ReturnType.MULTI,
                Collections.singletonList(key), argv);
        return ids.stream()
                .map(id -> {
                    String[] idp = id.split("-");
                    return new StreamMessageId(Long.parseLong(idp[0]), Long.parseLong(idp[1]));
                }).collect(Collectors.toList());
    }

    public Map<String, Object> XINFO_GROUPS(String key, String readGroupName) {
        String sha = loadedScriptIds.get(SCRIPTS.XINFO_GROUPS);
        List<Object> result = rScript.evalSha(RScript.Mode.READ_ONLY, sha, RScript.ReturnType.MULTI,
                Collections.singletonList(key), Collections.singletonList(readGroupName));
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
        String sha = loadedScriptIds.get(SCRIPTS.getStreamTailSize);
        return rScript.evalSha(RScript.Mode.READ_ONLY, sha, RScript.ReturnType.VALUE,
                Collections.singletonList(key), pseudoNextIdToDeliver);
    }
}
