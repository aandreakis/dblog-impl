package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.SourcePosition;

public interface SourceCheckpointCodec<P extends SourcePosition> {
  String encode(P position);

  P decode(String encoded);
}
