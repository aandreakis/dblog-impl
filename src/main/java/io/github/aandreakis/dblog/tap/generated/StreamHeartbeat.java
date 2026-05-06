
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
 * StreamHeartbeat
 * <p>
 * Liveness plus queue-depth sample. Emitted when the pump commits a batch and at least dblog.tap.heartbeat-interval has elapsed since the last heartbeat.
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
    "queue_depth",
    "queue_capacity"
})
public class StreamHeartbeat {

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
    private StreamHeartbeat.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_depth")
    private Long queueDepth;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_capacity")
    private Long queueCapacity;

    /**
     * No args constructor for use in serialization
     * 
     */
    public StreamHeartbeat() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param runId
     *     DBLog runtime run identifier.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public StreamHeartbeat(Long v, Instant ts, String runId, String sourceId, StreamHeartbeat.Kind kind, Long queueDepth, Long queueCapacity) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.queueDepth = queueDepth;
        this.queueCapacity = queueCapacity;
    }

    public static StreamHeartbeat.StreamHeartbeatBuilderBase builder() {
        return new StreamHeartbeat.StreamHeartbeatBuilder();
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
    public StreamHeartbeat.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(StreamHeartbeat.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_depth")
    public Long getQueueDepth() {
        return queueDepth;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_depth")
    public void setQueueDepth(Long queueDepth) {
        this.queueDepth = queueDepth;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_capacity")
    public Long getQueueCapacity() {
        return queueCapacity;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_capacity")
    public void setQueueCapacity(Long queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StreamHeartbeat.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("queueDepth");
        sb.append('=');
        sb.append(((this.queueDepth == null)?"<null>":this.queueDepth));
        sb.append(',');
        sb.append("queueCapacity");
        sb.append('=');
        sb.append(((this.queueCapacity == null)?"<null>":this.queueCapacity));
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
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.queueDepth == null)? 0 :this.queueDepth.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.queueCapacity == null)? 0 :this.queueCapacity.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof StreamHeartbeat) == false) {
            return false;
        }
        StreamHeartbeat rhs = ((StreamHeartbeat) other);
        return (((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.queueDepth == rhs.queueDepth)||((this.queueDepth!= null)&&this.queueDepth.equals(rhs.queueDepth))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.queueCapacity == rhs.queueCapacity)||((this.queueCapacity!= null)&&this.queueCapacity.equals(rhs.queueCapacity))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public enum Kind {

        STREAM_HEARTBEAT("stream.heartbeat");
        private final String value;
        private final static Map<String, StreamHeartbeat.Kind> CONSTANTS = new HashMap<String, StreamHeartbeat.Kind>();

        static {
            for (StreamHeartbeat.Kind c: values()) {
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
        public static StreamHeartbeat.Kind fromValue(String value) {
            StreamHeartbeat.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class StreamHeartbeatBuilder
        extends StreamHeartbeat.StreamHeartbeatBuilderBase<StreamHeartbeat>
    {


        public StreamHeartbeatBuilder() {
            super();
        }

        public StreamHeartbeatBuilder(Long v, Instant ts, String runId, String sourceId, StreamHeartbeat.Kind kind, Long queueDepth, Long queueCapacity) {
            super(v, ts, runId, sourceId, kind, queueDepth, queueCapacity);
        }

    }

    public static abstract class StreamHeartbeatBuilderBase<T extends StreamHeartbeat >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public StreamHeartbeatBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamHeartbeat.StreamHeartbeatBuilder.class)) {
                this.instance = ((T) new StreamHeartbeat());
            }
        }

        @SuppressWarnings("unchecked")
        public StreamHeartbeatBuilderBase(Long v, Instant ts, String runId, String sourceId, StreamHeartbeat.Kind kind, Long queueDepth, Long queueCapacity) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamHeartbeat.StreamHeartbeatBuilder.class)) {
                this.instance = ((T) new StreamHeartbeat(v, ts, runId, sourceId, kind, queueDepth, queueCapacity));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withV(Long v) {
            ((StreamHeartbeat) this.instance).v = v;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withSeq(Long seq) {
            ((StreamHeartbeat) this.instance).seq = seq;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withTs(Instant ts) {
            ((StreamHeartbeat) this.instance).ts = ts;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withRunId(String runId) {
            ((StreamHeartbeat) this.instance).runId = runId;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withSourceId(String sourceId) {
            ((StreamHeartbeat) this.instance).sourceId = sourceId;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withKind(StreamHeartbeat.Kind kind) {
            ((StreamHeartbeat) this.instance).kind = kind;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withQueueDepth(Long queueDepth) {
            ((StreamHeartbeat) this.instance).queueDepth = queueDepth;
            return this;
        }

        public StreamHeartbeat.StreamHeartbeatBuilderBase withQueueCapacity(Long queueCapacity) {
            ((StreamHeartbeat) this.instance).queueCapacity = queueCapacity;
            return this;
        }

    }

}
