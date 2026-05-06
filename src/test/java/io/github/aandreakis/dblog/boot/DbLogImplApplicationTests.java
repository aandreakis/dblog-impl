package io.github.aandreakis.dblog.boot;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

class DbLogImplApplicationTests {
  @Test
  void delegatesRuntimeModeToSpringBootLauncher() throws Exception {
    AtomicInteger springCalls = new AtomicInteger();

    int exitCode =
        DbLogImplApplication.run(
            new String[] {"--dblog.boot-mode=runtime"},
            args -> {
              springCalls.incrementAndGet();
              GenericApplicationContext context = new GenericApplicationContext();
              context.refresh();
              return context;
            });

    assertThat(exitCode).isZero();
    assertThat(springCalls.get()).isEqualTo(1);
  }

  @Test
  void defaultsToSpringBootLauncherWhenBootModeIsMissing() throws Exception {
    AtomicInteger springCalls = new AtomicInteger();

    int exitCode =
        DbLogImplApplication.run(
            new String[0],
            args -> {
              springCalls.incrementAndGet();
              GenericApplicationContext context = new GenericApplicationContext();
              context.refresh();
              return context;
            });

    assertThat(exitCode).isZero();
    assertThat(springCalls.get()).isEqualTo(1);
  }
}
