package io.github.yashrenhiet.lego.relay.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.yashrenhiet.lego.relay.delivery.DeliveryException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HttpSinkClassificationTest {

  @ParameterizedTest
  @ValueSource(ints = {200, 201, 202, 204})
  void successOn2xx(int status) {
    assertThatCode(() -> HttpSink.classify(status, "")).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 429, 500, 502, 503, 504})
  void retryableStatuses(int status) {
    DeliveryException e =
        catchThrowableOfType(DeliveryException.class, () -> HttpSink.classify(status, "busy"));
    assertThat(e).isNotNull();
    assertThat(e.isPermanent()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 409, 422})
  void permanentStatuses(int status) {
    DeliveryException e =
        catchThrowableOfType(DeliveryException.class, () -> HttpSink.classify(status, "bad"));
    assertThat(e).isNotNull();
    assertThat(e.isPermanent()).isTrue();
  }
}
