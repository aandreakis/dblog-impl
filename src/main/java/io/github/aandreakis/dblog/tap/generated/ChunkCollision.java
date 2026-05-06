
package io.github.aandreakis.dblog.tap.generated;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;


/**
 * ChunkCollision
 * <p>
 * An in-window CDC event caused a PK removal from the chunk refresh buffer.
 * 
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "v",
    "seq",
    "ts",
    "run_id",
    "source_id",
    "kind",
    "chunk_id",
    "excluded_pk",
    "cause_lsn",
    "cause_op",
    "cause_tx_id"
})
public class ChunkCollision {

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    @JsonPropertyDescription("Tap schema version; always 1.")
    private Long v;
    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    @JsonPropertyDescription("Monotonic per-run sequence number.")
    private Long seq;
    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    @JsonPropertyDescription("ISO-8601 UTC timestamp with microsecond resolution.")
    private Instant ts;
    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    @JsonPropertyDescription("DBLog runtime run identifier.")
    private String runId;
    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    @JsonPropertyDescription("Configured dblog.source.id.")
    private String sourceId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    private ChunkCollision.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    private Long chunkId;
    /**
     * Canonical PK literal of the row dropped from the buffer. Absent when the originating row-image has no captured schema.
     * 
     */
    @JsonProperty("excluded_pk")
    @JsonPropertyDescription("Canonical PK literal of the row dropped from the buffer. Absent when the originating row-image has no captured schema.")
    private String excludedPk;
    /**
     * LSN of the triggering in-window CDC event.
     * (Required)
     * 
     */
    @JsonProperty("cause_lsn")
    @JsonPropertyDescription("LSN of the triggering in-window CDC event.")
    private String causeLsn;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("cause_op")
    private ChunkCollision.CauseOp causeOp;
    /**
     * Transaction id of the triggering CDC when available.
     * 
     */
    @JsonProperty("cause_tx_id")
    @JsonPropertyDescription("Transaction id of the triggering CDC when available.")
    private String causeTxId;

    /**
     * No args constructor for use in serialization
     * 
     */
    public ChunkCollision() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param causeLsn
     *     LSN of the triggering in-window CDC event.
     * @param v
     *     Tap schema version; always 1.
     * @param runId
     *     DBLog runtime run identifier.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public ChunkCollision(Long v, Instant ts, String runId, String sourceId, ChunkCollision.Kind kind, Long chunkId, String causeLsn, ChunkCollision.CauseOp causeOp) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.chunkId = chunkId;
        this.causeLsn = causeLsn;
        this.causeOp = causeOp;
    }

    public static ChunkCollision.ChunkCollisionBuilderBase builder() {
        return new ChunkCollision.ChunkCollisionBuilder();
    }

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    public Long getV() {
        return v;
    }

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    public void setV(Long v) {
        this.v = v;
    }

    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    public Long getSeq() {
        return seq;
    }

    /**
     * Monotonic per-run sequence number.
     * 
     */
    @JsonProperty("seq")
    public void setSeq(Long seq) {
        this.seq = seq;
    }

    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    public Instant getTs() {
        return ts;
    }

    /**
     * ISO-8601 UTC timestamp with microsecond resolution.
     * (Required)
     * 
     */
    @JsonProperty("ts")
    public void setTs(Instant ts) {
        this.ts = ts;
    }

    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    public String getRunId() {
        return runId;
    }

    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    public void setRunId(String runId) {
        this.runId = runId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public String getSourceId() {
        return sourceId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public ChunkCollision.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(ChunkCollision.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    public Long getChunkId() {
        return chunkId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("chunk_id")
    public void setChunkId(Long chunkId) {
        this.chunkId = chunkId;
    }

    /**
     * Canonical PK literal of the row dropped from the buffer. Absent when the originating row-image has no captured schema.
     * 
     */
    @JsonProperty("excluded_pk")
    public String getExcludedPk() {
        return excludedPk;
    }

    /**
     * Canonical PK literal of the row dropped from the buffer. Absent when the originating row-image has no captured schema.
     * 
     */
    @JsonProperty("excluded_pk")
    public void setExcludedPk(String excludedPk) {
        this.excludedPk = excludedPk;
    }

    /**
     * LSN of the triggering in-window CDC event.
     * (Required)
     * 
     */
    @JsonProperty("cause_lsn")
    public String getCauseLsn() {
        return causeLsn;
    }

    /**
     * LSN of the triggering in-window CDC event.
     * (Required)
     * 
     */
    @JsonProperty("cause_lsn")
    public void setCauseLsn(String causeLsn) {
        this.causeLsn = causeLsn;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("cause_op")
    public ChunkCollision.CauseOp getCauseOp() {
        return causeOp;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("cause_op")
    public void setCauseOp(ChunkCollision.CauseOp causeOp) {
        this.causeOp = causeOp;
    }

    /**
     * Transaction id of the triggering CDC when available.
     * 
     */
    @JsonProperty("cause_tx_id")
    public String getCauseTxId() {
        return causeTxId;
    }

    /**
     * Transaction id of the triggering CDC when available.
     * 
     */
    @JsonProperty("cause_tx_id")
    public void setCauseTxId(String causeTxId) {
        this.causeTxId = causeTxId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ChunkCollision.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("v");
        sb.append('=');
        sb.append(((this.v == null)?"<null>":this.v));
        sb.append(',');
        sb.append("seq");
        sb.append('=');
        sb.append(((this.seq == null)?"<null>":this.seq));
        sb.append(',');
        sb.append("ts");
        sb.append('=');
        sb.append(((this.ts == null)?"<null>":this.ts));
        sb.append(',');
        sb.append("runId");
        sb.append('=');
        sb.append(((this.runId == null)?"<null>":this.runId));
        sb.append(',');
        sb.append("sourceId");
        sb.append('=');
        sb.append(((this.sourceId == null)?"<null>":this.sourceId));
        sb.append(',');
        sb.append("kind");
        sb.append('=');
        sb.append(((this.kind == null)?"<null>":this.kind));
        sb.append(',');
        sb.append("chunkId");
        sb.append('=');
        sb.append(((this.chunkId == null)?"<null>":this.chunkId));
        sb.append(',');
        sb.append("excludedPk");
        sb.append('=');
        sb.append(((this.excludedPk == null)?"<null>":this.excludedPk));
        sb.append(',');
        sb.append("causeLsn");
        sb.append('=');
        sb.append(((this.causeLsn == null)?"<null>":this.causeLsn));
        sb.append(',');
        sb.append("causeOp");
        sb.append('=');
        sb.append(((this.causeOp == null)?"<null>":this.causeOp));
        sb.append(',');
        sb.append("causeTxId");
        sb.append('=');
        sb.append(((this.causeTxId == null)?"<null>":this.causeTxId));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.sourceId == null)? 0 :this.sourceId.hashCode()));
        result = ((result* 31)+((this.causeLsn == null)? 0 :this.causeLsn.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.chunkId == null)? 0 :this.chunkId.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.causeTxId == null)? 0 :this.causeTxId.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        result = ((result* 31)+((this.excludedPk == null)? 0 :this.excludedPk.hashCode()));
        result = ((result* 31)+((this.causeOp == null)? 0 :this.causeOp.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ChunkCollision) == false) {
            return false;
        }
        ChunkCollision rhs = ((ChunkCollision) other);
        return ((((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.causeLsn == rhs.causeLsn)||((this.causeLsn!= null)&&this.causeLsn.equals(rhs.causeLsn))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.chunkId == rhs.chunkId)||((this.chunkId!= null)&&this.chunkId.equals(rhs.chunkId))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.causeTxId == rhs.causeTxId)||((this.causeTxId!= null)&&this.causeTxId.equals(rhs.causeTxId))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))))&&((this.excludedPk == rhs.excludedPk)||((this.excludedPk!= null)&&this.excludedPk.equals(rhs.excludedPk))))&&((this.causeOp == rhs.causeOp)||((this.causeOp!= null)&&this.causeOp.equals(rhs.causeOp))));
    }

    public enum CauseOp {

        INSERT("INSERT"),
        UPDATE("UPDATE"),
        DELETE("DELETE");
        private final String value;
        private final static Map<String, ChunkCollision.CauseOp> CONSTANTS = new HashMap<String, ChunkCollision.CauseOp>();

        static {
            for (ChunkCollision.CauseOp c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        CauseOp(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static ChunkCollision.CauseOp fromValue(String value) {
            ChunkCollision.CauseOp constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class ChunkCollisionBuilder
        extends ChunkCollision.ChunkCollisionBuilderBase<ChunkCollision>
    {


        public ChunkCollisionBuilder() {
            super();
        }

        public ChunkCollisionBuilder(Long v, Instant ts, String runId, String sourceId, ChunkCollision.Kind kind, Long chunkId, String causeLsn, ChunkCollision.CauseOp causeOp) {
            super(v, ts, runId, sourceId, kind, chunkId, causeLsn, causeOp);
        }

    }

    public static abstract class ChunkCollisionBuilderBase<T extends ChunkCollision >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public ChunkCollisionBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkCollision.ChunkCollisionBuilder.class)) {
                this.instance = ((T) new ChunkCollision());
            }
        }

        @SuppressWarnings("unchecked")
        public ChunkCollisionBuilderBase(Long v, Instant ts, String runId, String sourceId, ChunkCollision.Kind kind, Long chunkId, String causeLsn, ChunkCollision.CauseOp causeOp) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ChunkCollision.ChunkCollisionBuilder.class)) {
                this.instance = ((T) new ChunkCollision(v, ts, runId, sourceId, kind, chunkId, causeLsn, causeOp));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withV(Long v) {
            ((ChunkCollision) this.instance).v = v;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withSeq(Long seq) {
            ((ChunkCollision) this.instance).seq = seq;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withTs(Instant ts) {
            ((ChunkCollision) this.instance).ts = ts;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withRunId(String runId) {
            ((ChunkCollision) this.instance).runId = runId;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withSourceId(String sourceId) {
            ((ChunkCollision) this.instance).sourceId = sourceId;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withKind(ChunkCollision.Kind kind) {
            ((ChunkCollision) this.instance).kind = kind;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withChunkId(Long chunkId) {
            ((ChunkCollision) this.instance).chunkId = chunkId;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withExcludedPk(String excludedPk) {
            ((ChunkCollision) this.instance).excludedPk = excludedPk;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withCauseLsn(String causeLsn) {
            ((ChunkCollision) this.instance).causeLsn = causeLsn;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withCauseOp(ChunkCollision.CauseOp causeOp) {
            ((ChunkCollision) this.instance).causeOp = causeOp;
            return this;
        }

        public ChunkCollision.ChunkCollisionBuilderBase withCauseTxId(String causeTxId) {
            ((ChunkCollision) this.instance).causeTxId = causeTxId;
            return this;
        }

    }

    public enum Kind {

        CHUNK_COLLISION("chunk.collision");
        private final String value;
        private final static Map<String, ChunkCollision.Kind> CONSTANTS = new HashMap<String, ChunkCollision.Kind>();

        static {
            for (ChunkCollision.Kind c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Kind(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return this.value;
        }

        @JsonValue
        public String value() {
            return this.value;
        }

        @JsonCreator
        public static ChunkCollision.Kind fromValue(String value) {
            ChunkCollision.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
