/**
 * Runtime loops that drive streaming and request flows. {@code RuntimeStreamingPump} owns event
 * emission and checkpoint advancement;
 * {@link io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump} interleaves
 * operator request batches with that streaming work.
 */
package io.github.aandreakis.dblog.runtime.loop;
