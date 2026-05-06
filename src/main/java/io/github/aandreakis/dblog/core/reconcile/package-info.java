/**
 * Watermark-window reconciliation: the DBLog state machine that suppresses snapshot rows colliding
 * with same-table log events inside a low/high watermark window.
 * {@link io.github.aandreakis.dblog.core.reconcile.WindowReconciler} owns the algorithm.
 */
package io.github.aandreakis.dblog.core.reconcile;
