package io.github.yashrenhiet.lego.relay.sink;

import io.github.yashrenhiet.lego.relay.config.LegoProperties;
import io.github.yashrenhiet.lego.relay.delivery.DeliveryException;
import io.github.yashrenhiet.lego.relay.delivery.Sink;
import io.github.yashrenhiet.lego.relay.outbox.OutboxRecord;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Sends the payload to an HTTP endpoint.
 *
 * <ul>
 *   <li>2xx: delivered
 *   <li>408, 429, 5xx, timeouts and I/O errors: retried with backoff
 *   <li>any other status (400, 401, 404, 422...): permanent, dead-lettered immediately
 * </ul>
 *
 * Headers sent: the configured static headers, the event's own headers, {@code Lego-Event-Id}
 * and {@code Lego-Event-Key}. Receivers should de-duplicate on {@code Lego-Event-Id}.
 */
public class HttpSink implements Sink {

  private final HttpClient client;
  private final URI url;
  private final String method;
  private final Map<String, String> headers;
  private final Duration timeout;

  public HttpSink(HttpClient client, LegoProperties.Http config) {
    if (config.url() == null) {
      throw new IllegalArgumentException("HTTP destination requires 'http.url'");
    }
    this.client = client;
    this.url = config.url();
    this.method = config.method();
    this.headers = Map.copyOf(config.headers());
    this.timeout = config.timeout();
  }

  @Override
  public void send(OutboxRecord event) throws DeliveryException {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(url)
            .timeout(timeout)
            .method(method, HttpRequest.BodyPublishers.ofString(event.payload()))
            .header("Content-Type", "application/json")
            .header("Lego-Event-Id", event.eventId().toString())
            .header("Lego-Event-Key", event.key());
    headers.forEach(request::setHeader);
    event.headers().forEach(request::setHeader);

    HttpResponse<String> response;
    try {
      response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw DeliveryException.retryable("HTTP call to " + url + " failed: " + e, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw DeliveryException.retryable("Interrupted while calling " + url, e);
    }
    classify(response.statusCode(), response.body());
  }

  static void classify(int status, String body) throws DeliveryException {
    if (status >= 200 && status < 300) {
      return;
    }
    String message = "HTTP " + status + ": " + abbreviate(body);
    if (status == 408 || status == 429 || status >= 500) {
      throw DeliveryException.retryable(message, null);
    }
    throw DeliveryException.permanent(message, null);
  }

  private static String abbreviate(String body) {
    if (body == null || body.isBlank()) {
      return "<empty body>";
    }
    return body.length() <= 500 ? body : body.substring(0, 500) + "...";
  }
}
