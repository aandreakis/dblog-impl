package io.github.aandreakis.dblog.config;

import jakarta.validation.constraints.Min;
import java.nio.file.Path;

/** Local HTTP control-plane configuration. */
public class DbLogControlPlaneProperties {
  private boolean enabled = false;
  private String host = "127.0.0.1";

  /**
   * Opt-in escape hatch for binding the control plane to a non-loopback address. The control
   * plane does not authenticate callers, so a non-loopback bind exposes runtime inspection and
   * request-submission endpoints to anyone who can reach the port. The only legitimate use is
   * inside a container where Docker (or equivalent) enforces host-side port-publishing isolation;
   * see ops/docker/examples/. Default is {@code false} — any non-loopback host value fails
   * closed at startup unless this is explicitly set to {@code true}.
   */
  private boolean allowNonLoopback = false;

  @Min(0)
  private int port = 8085;

  /**
   * If set, the bound control-plane port is written to this file once {@code start()} returns.
   * Useful with {@code dblog.control-plane.port=0} (OS-assigned port) when an external supervisor
   * — a deployment script, a sidecar, or an integration test — needs to discover the port. The
   * file is written atomically (temp + rename) so a concurrent reader cannot observe a partial
   * value, and is deleted on graceful shutdown.
   */
  private Path portFile;

  @Min(1)
  private int executorMaxThreads = 8;

  @Min(1)
  private int executorQueueCapacity = 64;

  @Min(1)
  private int maxRequestBodyBytes = 1_048_576;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public boolean isAllowNonLoopback() {
    return allowNonLoopback;
  }

  public void setAllowNonLoopback(boolean allowNonLoopback) {
    this.allowNonLoopback = allowNonLoopback;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public Path getPortFile() {
    return portFile;
  }

  public void setPortFile(Path portFile) {
    this.portFile = portFile;
  }

  public int getExecutorMaxThreads() {
    return executorMaxThreads;
  }

  public void setExecutorMaxThreads(int executorMaxThreads) {
    this.executorMaxThreads = executorMaxThreads;
  }

  public int getExecutorQueueCapacity() {
    return executorQueueCapacity;
  }

  public void setExecutorQueueCapacity(int executorQueueCapacity) {
    this.executorQueueCapacity = executorQueueCapacity;
  }

  public int getMaxRequestBodyBytes() {
    return maxRequestBodyBytes;
  }

  public void setMaxRequestBodyBytes(int maxRequestBodyBytes) {
    this.maxRequestBodyBytes = maxRequestBodyBytes;
  }
}
