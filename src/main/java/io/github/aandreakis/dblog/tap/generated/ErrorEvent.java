
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
 * ErrorEvent
 * <p>
 * A fail-closed runtime boundary was hit.
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
    "class",
    "message",
    "context"
})
public class ErrorEvent {

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
    private java.lang.String runId;
    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    @JsonPropertyDescription("Configured dblog.source.id.")
    private java.lang.String sourceId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    private ErrorEvent.Kind kind;
    /**
     * Exception class name, e.g. WatermarkSequenceException.
     * (Required)
     * 
     */
    @JsonProperty("class")
    @JsonPropertyDescription("Exception class name, e.g. WatermarkSequenceException.")
    private java.lang.String _class;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    private java.lang.String message;
    /**
     * Structured detail.
     * 
     */
    @JsonProperty("context")
    @JsonPropertyDescription("Structured detail.")
    private Map<String, Object> context;

    /**
     * No args constructor for use in serialization
     * 
     */
    public ErrorEvent() {
    }

    /**
     * 
     * @param sourceId
     *     Configured dblog.source.id.
     * @param v
     *     Tap schema version; always 1.
     * @param runId
     *     DBLog runtime run identifier.
     * @param _class
     *     Exception class name, e.g. WatermarkSequenceException.
     * @param ts
     *     ISO-8601 UTC timestamp with microsecond resolution.
     */
    public ErrorEvent(Long v, Instant ts, java.lang.String runId, java.lang.String sourceId, ErrorEvent.Kind kind, java.lang.String _class, java.lang.String message) {
        super();
        this.v = v;
        this.ts = ts;
        this.runId = runId;
        this.sourceId = sourceId;
        this.kind = kind;
        this._class = _class;
        this.message = message;
    }

    public static ErrorEvent.ErrorEventBuilderBase builder() {
        return new ErrorEvent.ErrorEventBuilder();
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
    public java.lang.String getRunId() {
        return runId;
    }

    /**
     * DBLog runtime run identifier.
     * (Required)
     * 
     */
    @JsonProperty("run_id")
    public void setRunId(java.lang.String runId) {
        this.runId = runId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public java.lang.String getSourceId() {
        return sourceId;
    }

    /**
     * Configured dblog.source.id.
     * (Required)
     * 
     */
    @JsonProperty("source_id")
    public void setSourceId(java.lang.String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public ErrorEvent.Kind getKind() {
        return kind;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("kind")
    public void setKind(ErrorEvent.Kind kind) {
        this.kind = kind;
    }

    /**
     * Exception class name, e.g. WatermarkSequenceException.
     * (Required)
     * 
     */
    @JsonProperty("class")
    public java.lang.String getClass_() {
        return _class;
    }

    /**
     * Exception class name, e.g. WatermarkSequenceException.
     * (Required)
     * 
     */
    @JsonProperty("class")
    public void setClass_(java.lang.String _class) {
        this._class = _class;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    public java.lang.String getMessage() {
        return message;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("message")
    public void setMessage(java.lang.String message) {
        this.message = message;
    }

    /**
     * Structured detail.
     * 
     */
    @JsonProperty("context")
    public Map<String, Object> getContext() {
        return context;
    }

    /**
     * Structured detail.
     * 
     */
    @JsonProperty("context")
    public void setContext(Map<String, Object> context) {
        this.context = context;
    }

    @Override
    public java.lang.String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ErrorEvent.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
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
        sb.append("_class");
        sb.append('=');
        sb.append(((this._class == null)?"<null>":this._class));
        sb.append(',');
        sb.append("message");
        sb.append('=');
        sb.append(((this.message == null)?"<null>":this.message));
        sb.append(',');
        sb.append("context");
        sb.append('=');
        sb.append(((this.context == null)?"<null>":this.context));
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
        result = ((result* 31)+((this.context == null)? 0 :this.context.hashCode()));
        result = ((result* 31)+((this.runId == null)? 0 :this.runId.hashCode()));
        result = ((result* 31)+((this._class == null)? 0 :this._class.hashCode()));
        result = ((result* 31)+((this.message == null)? 0 :this.message.hashCode()));
        result = ((result* 31)+((this.seq == null)? 0 :this.seq.hashCode()));
        result = ((result* 31)+((this.ts == null)? 0 :this.ts.hashCode()));
        return result;
    }

    @Override
    public boolean equals(java.lang.Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ErrorEvent) == false) {
            return false;
        }
        ErrorEvent rhs = ((ErrorEvent) other);
        return ((((((((((this.sourceId == rhs.sourceId)||((this.sourceId!= null)&&this.sourceId.equals(rhs.sourceId)))&&((this.v == rhs.v)||((this.v!= null)&&this.v.equals(rhs.v))))&&((this.kind == rhs.kind)||((this.kind!= null)&&this.kind.equals(rhs.kind))))&&((this.context == rhs.context)||((this.context!= null)&&this.context.equals(rhs.context))))&&((this.runId == rhs.runId)||((this.runId!= null)&&this.runId.equals(rhs.runId))))&&((this._class == rhs._class)||((this._class!= null)&&this._class.equals(rhs._class))))&&((this.message == rhs.message)||((this.message!= null)&&this.message.equals(rhs.message))))&&((this.seq == rhs.seq)||((this.seq!= null)&&this.seq.equals(rhs.seq))))&&((this.ts == rhs.ts)||((this.ts!= null)&&this.ts.equals(rhs.ts))));
    }

    public static class ErrorEventBuilder
        extends ErrorEvent.ErrorEventBuilderBase<ErrorEvent>
    {


        public ErrorEventBuilder() {
            super();
        }

        public ErrorEventBuilder(Long v, Instant ts, java.lang.String runId, java.lang.String sourceId, ErrorEvent.Kind kind, java.lang.String _class, java.lang.String message) {
            super(v, ts, runId, sourceId, kind, _class, message);
        }

    }

    public static abstract class ErrorEventBuilderBase<T extends ErrorEvent >{

        protected T instance;

        @SuppressWarnings("unchecked")
        public ErrorEventBuilderBase() {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ErrorEvent.ErrorEventBuilder.class)) {
                this.instance = ((T) new ErrorEvent());
            }
        }

        @SuppressWarnings("unchecked")
        public ErrorEventBuilderBase(Long v, Instant ts, java.lang.String runId, java.lang.String sourceId, ErrorEvent.Kind kind, java.lang.String _class, java.lang.String message) {
            // Skip initialization when called from subclass
            if (this.getClass().equals(ErrorEvent.ErrorEventBuilder.class)) {
                this.instance = ((T) new ErrorEvent(v, ts, runId, sourceId, kind, _class, message));
            }
        }

        public T build() {
            T result;
            result = this.instance;
            this.instance = null;
            return result;
        }

        public ErrorEvent.ErrorEventBuilderBase withV(Long v) {
            ((ErrorEvent) this.instance).v = v;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withSeq(Long seq) {
            ((ErrorEvent) this.instance).seq = seq;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withTs(Instant ts) {
            ((ErrorEvent) this.instance).ts = ts;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withRunId(java.lang.String runId) {
            ((ErrorEvent) this.instance).runId = runId;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withSourceId(java.lang.String sourceId) {
            ((ErrorEvent) this.instance).sourceId = sourceId;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withKind(ErrorEvent.Kind kind) {
            ((ErrorEvent) this.instance).kind = kind;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withClass(java.lang.String _class) {
            ((ErrorEvent) this.instance)._class = _class;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withMessage(java.lang.String message) {
            ((ErrorEvent) this.instance).message = message;
            return this;
        }

        public ErrorEvent.ErrorEventBuilderBase withContext(Map<String, Object> context) {
            ((ErrorEvent) this.instance).context = context;
            return this;
        }

    }

    public enum Kind {

        ERROR("error");
        private final java.lang.String value;
        private final static Map<java.lang.String, ErrorEvent.Kind> CONSTANTS = new HashMap<java.lang.String, ErrorEvent.Kind>();

        static {
            for (ErrorEvent.Kind c: values()) {
                CONSTANTS.put(c.value, c);
            }
        }

        Kind(java.lang.String value) {
            this.value = value;
        }

        @Override
        public java.lang.String toString() {
            return this.value;
        }

        @JsonValue
        public java.lang.String value() {
            return this.value;
        }

        @JsonCreator
        public static ErrorEvent.Kind fromValue(java.lang.String value) {
            ErrorEvent.Kind constant = CONSTANTS.get(value);
            if (constant == null) {
                throw new IllegalArgumentException(value);
            } else {
                return constant;
            }
        }

    }

}
