
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
 * StreamResumed
 * <p>
 * The producer is no longer blocked. Emitted out of band; seq is null.
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
    "standby_total_ms"
})
public class StreamResumed {

    /**
     * Tap schema version; always 1.
     * (Required)
     * 
     */
    @JsonProperty("v")
    @JsonPropertyDescription("Tap schema version; always 1.")
    private Long v;
    /**
     * Null on stream.resumed — the event is emitted out of band.
     * 
     */
    @JsonProperty("seq")
    @JsonPropertyDescription("Null on stream.resumed \u2014 the event is emitted out of band.")
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
    private StreamResumed.Kind kind;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("standby_total_ms")
    private Long standbyTotalMs;

    /**
     * No args constructor for use in serialization
     * 
     */
    public StreamResumed() {
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
    public StreamResumed(Long v, Instant ts, String runId, String sourceId, StreamResumed.Kind kind, Long standbyTotalMs) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this.standbyTotalMs = standbyTotalMs;
    }

    public static StreamResumed.StreamResumedBuilderBase builder() {
        return new StreamResumed.StreamResumedBuilder();
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
     * Null on stream.resumed — the event is emitted out of band.
     * 
     */
    @JsonProperty("seq")
    public Object getSeq() {
        return seq;
    }

    /**
     * Null on stream.resumed — the event is emitted out of band.
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
    public StreamResumed.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(StreamResumed.Kind kind) {
        this.kind = kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("standby_total_ms")
    public Long getStandbyTotalMs() {
        return standbyTotalMs;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("standby_total_ms")
    public void setStandbyTotalMs(Long standbyTotalMs) {
        this.standbyTotalMs = standbyTotalMs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StreamResumed.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("standbyTotalMs");
        sb.append('=');
        sb.append(((this.standbyTotalMs == null)?"<null>":this.standbyTotalMs));
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
        result = ((result* 31)+((this.standbyTotalMs == null)? 0 :this.standbyTotalMs.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof StreamResumed) == false) {
            return false;
        }
        StreamResumed rhs = ((StreamResumed) other);
        return ((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.standbyTotalMs == rhs.standbyTotalMs)||((this.standbyTotalMs!= null)&&this.standbyTotalMs.equals(rhs.standbyTotalMs))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public enum Kind {

        STREAM_RESUMED("stream.resumed");
        private final String value;
        private final static Map<String, StreamResumed.Kind> CONSTANTS = new HashMap<String, StreamResumed.Kind>();

        static {
            for (StreamResumed.Kind c: values()) {
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
        public static StreamResumed.Kind fromValue(String value) {
            StreamResumed.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

    public static class StreamResumedBuilder
        extends StreamResumed.StreamResumedBuilderBase<StreamResumed>
    {


        public StreamResumedBuilder() {
            super();
        }

        public StreamResumedBuilder(Long v, Instant ts, String runId, String sourceId, StreamResumed.Kind kind, Long standbyTotalMs) {
            super(v, ts, runId, sourceId, kind, standbyTotalMs);
        }

    }

    public static abstract class StreamResumedBuilderBase<T extends StreamResumed >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public StreamResumedBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamResumed.StreamResumedBuilder.class)) {
                this.instance = ((T) new StreamResumed());
            }
        }

        @SuppressWarnings("unchecked")
        public StreamResumedBuilderBase(Long v, Instant ts, String runId, String sourceId, StreamResumed.Kind kind, Long standbyTotalMs) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(StreamResumed.StreamResumedBuilder.class)) {
                this.instance = ((T) new StreamResumed(v, ts, runId, sourceId, kind, standbyTotalMs));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public StreamResumed.StreamResumedBuilderBase withV(Long v) {
            ((StreamResumed) this.instance).v = v;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withSeq(Object seq) {
            ((StreamResumed) this.instance).seq = seq;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withTs(Instant ts) {
            ((StreamResumed) this.instance).ts = ts;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withRunId(String runId) {
            ((StreamResumed) this.instance).runId = runId;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withSourceId(String sourceId) {
            ((StreamResumed) this.instance).sourceId = sourceId;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withKind(StreamResumed.Kind kind) {
            ((StreamResumed) this.instance).kind = kind;
            return this;
        }

        public StreamResumed.StreamResumedBuilderBase withStandbyTotalMs(Long standbyTotalMs) {
            ((StreamResumed) this.instance).standbyTotalMs = standbyTotalMs;
            return this;
        }

    }

}
