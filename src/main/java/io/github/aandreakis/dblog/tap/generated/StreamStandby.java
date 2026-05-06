
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
 * StreamStandby
 * <p>
 * The producer has been blocked on a full queue for at least dblog.tap.standby-threshold-ms. Emitted out of band; seq is null.
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
    "queue_full_ms",
    "queue_capacity",
    "reason",
    "message"
})
public class StreamStandby {

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    @JsonPropertyDescription("Tap schema version; always 1.")
    private Long v;
    /**
     * Null on stream.standby — the event is emitted out of band.
     * 
     */
    @JsonProperty("seq")
    @JsonPropertyDescription("Null on stream.standby \u2014 the event is emitted out of band.")
    private Object seq;
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
    private StreamStandby.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_full_ms")
    private Long queueFullMs;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_capacity")
    private Long queueCapacity;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    private StreamStandby.Reason reason;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    private String message;

    /**
     * No args constructor for use in serialization
     * 
     */
    public StreamStandby() {
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
    public StreamStandby(Long v, Instant ts, String runId, String sourceId, StreamStandby.Kind kind, Long queueFullMs, Long queueCapacity, StreamStandby.Reason reason, String message) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.queueFullMs = queueFullMs;
        this.queueCapacity = queueCapacity;
        this.reason = reason;
        this.message = message;
    }

    public static StreamStandby.StreamStandbyBuilderBase builder() {
        return new StreamStandby.StreamStandbyBuilder();
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
     * Null on stream.standby — the event is emitted out of band.
     * 
     */
    @JsonProperty("seq")
    public Object getSeq() {
        return seq;
    }

    /**
     * Null on stream.standby — the event is emitted out of band.
     * 
     */
    @JsonProperty("seq")
    public void setSeq(Object seq) {
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
    public StreamStandby.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(StreamStandby.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_full_ms")
    public Long getQueueFullMs() {
        return queueFullMs;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("queue_full_ms")
    public void setQueueFullMs(Long queueFullMs) {
        this.queueFullMs = queueFullMs;
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

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    public StreamStandby.Reason getReason() {
        return reason;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("reason")
    public void setReason(StreamStandby.Reason reason) {
        this.reason = reason;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    public String getMessage() {
        return message;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StreamStandby.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("queueFullMs");
        sb.append('=');
        sb.append(((this.queueFullMs == null)?"<null>":this.queueFullMs));
        sb.append(',');
        sb.append("queueCapacity");
        sb.append('=');
        sb.append(((this.queueCapacity == null)?"<null>":this.queueCapacity));
        sb.append(',');
        sb.append("reason");
        sb.append('=');
        sb.append(((this.reason == null)?"<null>":this.reason));
        sb.append(',');
        sb.append("message");
        sb.append('=');
        sb.append(((this.message == null)?"<null>":this.message));
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
        result = ((result* 31)+((this.reason == null)? 0 :this.reason.hashCode()));
        result = ((result* 31)+((this.v == null)? 0 :this.v.hashCode()));
        result = ((result* 31)+((this.kind == null)? 0 :this.kind.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.queueCapacity == null)? 0 :this.queueCapacity.hashCode()));
        result = ((result* 31)+((this.message == null)? 0 :this.message.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.queueFullMs == null)? 0 :this.queueFullMs.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof StreamStandby) == false) {
            return false;
        }
        StreamStandby rhs = ((StreamStandby) other);
        return (((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.reason == rhs.reason)||((this.reason!= null)&&this.reason.equals(rhs.reason))))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.queueCapacity == rhs.queueCapacity)||((this.queueCapacity!= null)&&this.queueCapacity.equals(rhs.queueCapacity))))&&((this.message == rhs.message)||((this.message!= null)&&this.message.equals(rhs.message))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.queueFullMs == rhs.queueFullMs)||((this.queueFullMs!= null)&&this.queueFullMs.equals(rhs.queueFullMs))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public enum Kind {

        STREAM_STANDBY("stream.standby");
        private final String value;
        private final static Map<String, StreamStandby.Kind> CONSTANTS = new HashMap<String, StreamStandby.Kind>();

        static {
            for (StreamStandby.Kind c: values()) {
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
        public static StreamStandby.Kind fromValue(String value) {
            StreamStandby.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public enum Reason {

        QUEUE_FULL("queue_full");
        private final String value;
        private final static Map<String, StreamStandby.Reason> CONSTANTS = new HashMap<String, StreamStandby.Reason>();

        static {
            for (StreamStandby.Reason c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Reason(String value) {
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
        public static StreamStandby.Reason fromValue(String value) {
            StreamStandby.Reason constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class StreamStandbyBuilder
        extends StreamStandby.StreamStandbyBuilderBase<StreamStandby>
    {


        public StreamStandbyBuilder() {
            super();
        }

        public StreamStandbyBuilder(Long v, Instant ts, String runId, String sourceId, StreamStandby.Kind kind, Long queueFullMs, Long queueCapacity, StreamStandby.Reason reason, String message) {
            super(v, ts, runId, sourceId, kind, queueFullMs, queueCapacity, reason, message);
        }

    }

    public static abstract class StreamStandbyBuilderBase<T extends StreamStandby >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public StreamStandbyBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamStandby.StreamStandbyBuilder.class)) {
                this.instance = ((T) new StreamStandby());
            }
        }

        @SuppressWarnings("unchecked")
        public StreamStandbyBuilderBase(Long v, Instant ts, String runId, String sourceId, StreamStandby.Kind kind, Long queueFullMs, Long queueCapacity, StreamStandby.Reason reason, String message) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamStandby.StreamStandbyBuilder.class)) {
                this.instance = ((T) new StreamStandby(v, ts, runId, sourceId, kind, queueFullMs, queueCapacity, reason, message));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public StreamStandby.StreamStandbyBuilderBase withV(Long v) {
            ((StreamStandby) this.instance).v = v;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withSeq(Object seq) {
            ((StreamStandby) this.instance).seq = seq;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withTs(Instant ts) {
            ((StreamStandby) this.instance).ts = ts;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withRunId(String runId) {
            ((StreamStandby) this.instance).runId = runId;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withSourceId(String sourceId) {
            ((StreamStandby) this.instance).sourceId = sourceId;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withKind(StreamStandby.Kind kind) {
            ((StreamStandby) this.instance).kind = kind;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withQueueFullMs(Long queueFullMs) {
            ((StreamStandby) this.instance).queueFullMs = queueFullMs;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withQueueCapacity(Long queueCapacity) {
            ((StreamStandby) this.instance).queueCapacity = queueCapacity;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withReason(StreamStandby.Reason reason) {
            ((StreamStandby) this.instance).reason = reason;
            return this;
        }

        public StreamStandby.StreamStandbyBuilderBase withMessage(String message) {
            ((StreamStandby) this.instance).message = message;
            return this;
        }

    }

}
