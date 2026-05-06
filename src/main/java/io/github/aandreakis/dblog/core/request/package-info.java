/**
 * Dump and targeted-repair request coordination: opens watermark windows, drives chunks through
 * the reconciler, and pins durable per-table progress at chunk boundaries. Entry types:
 * {@link io.github.aandreakis.dblog.core.request.DumpRequest},
 * {@link io.github.aandreakis.dblog.core.request.DefaultDumpWindowCoordinator},
 * {@link io.github.aandreakis.dblog.core.request.DefaultTargetedRepairCoordinator}.
 */
package io.github.aandreakis.dblog.core.request;
