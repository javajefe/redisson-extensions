package tech.javajefe.redis.redisson.extensions;

import org.redisson.api.RScript;

/**
 * Created by Alexander Bukarev on 08.03.2019.
 */
enum Scripts {

    batchXADD("/lua/batchXADD.lua", RScript.Mode.READ_WRITE, RScript.ReturnType.MULTI),
    XINFO_GROUPS("/lua/XINFO_GROUPS.lua", RScript.Mode.READ_ONLY, RScript.ReturnType.MULTI),
    getStreamTailSize("/lua/getStreamTailSize.lua", RScript.Mode.READ_ONLY, RScript.ReturnType.VALUE);

    private String path;
    private RScript.Mode mode;
    private RScript.ReturnType returnType;

    Scripts(String path, RScript.Mode mode, RScript.ReturnType returnType) {
        this.path = path;
        this.mode = mode;
        this.returnType = returnType;
    }

    String getPath() {
        return this.path;
    }

    public RScript.Mode getMode() {
        return mode;
    }

    public RScript.ReturnType getReturnType() {
        return returnType;
    }
}
