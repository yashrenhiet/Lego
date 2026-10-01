package io.github.yashrenhiet.lego.relay.admin;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoints that change outbox data. Disabled unless {@code lego.admin.write-enabled=true}, so a
 * default deployment can't have its dead letters deleted by anyone who can reach the port.
 *
 * <p>No authentication is built in; when enabling, expose it only behind your gateway.
 */
@RestController
@RequestMapping("/admin/dead-events")
@ConditionalOnProperty(name = "lego.admin.write-enabled", havingValue = "true")
public class DeadLetterCommandController {

  private final AdminRepository repository;

  public DeadLetterCommandController(AdminRepository repository) {
    this.repository = repository;
  }

  /** Re-queues dead events with a fresh retry budget. */
  @PostMapping("/replay")
  public Result replay(@Valid @RequestBody EventIds request) {
    return new Result(repository.replay(request.eventIds()));
  }

  /** Permanently deletes dead events. */
  @PostMapping("/discard")
  public Result discard(@Valid @RequestBody EventIds request) {
    return new Result(repository.discard(request.eventIds()));
  }

  public record EventIds(@NotEmpty @Size(max = 1000) List<UUID> eventIds) {}

  public record Result(int affected) {}
}
