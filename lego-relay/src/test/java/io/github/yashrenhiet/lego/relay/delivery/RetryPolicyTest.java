package io.github.yashrenhiet.lego.relay.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  private final RetryPolicy policy =
      new RetryPolicy(new LegoProperties.Retry(5, Duration.ofSeconds(1), Duration.ofSeconds(5), 2.0));

  @ParameterizedTest(name = "attempt {0} -> {1} ms")
  @CsvSource({"1, 1000", "2, 2000", "3, 4000", "4, 5000"})
  void backsOffExponentiallyUpToTheCap(int attempts, long expectedMillis) {
    RetryPolicy.Decision decision = policy.afterFailure(attempts, false);

    assertThat(decision.isDeadLetter()).isFalse();
    assertThat(decision.backoff()).isEqualTo(Duration.ofMillis(expectedMillis));
  }

  @Test
  void deadLettersWhenAttemptsAreExhausted() {
    assertThat(policy.afterFailure(5, false).isDeadLetter()).isTrue();
  }

  @Test
  void deadLettersPermanentFailuresImmediately() {
    assertThat(policy.afterFailure(1, true).isDeadLetter()).isTrue();
  }
}
