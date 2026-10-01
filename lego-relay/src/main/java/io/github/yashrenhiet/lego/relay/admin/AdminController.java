package io.github.yashrenhiet.lego.relay.admin;

import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DeadEvent;
import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DestinationStats;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;
import java.util.UUID;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operations API.
 *
 * <p>No authentication is built in; expose it only on an internal network or behind your
 * gateway.
 */
@Validated
@RestController
@RequestMapping("/admin")
public class AdminController {

  private final AdminRepository repository;

  public AdminController(AdminRepository repository) {
    this.repository = repository;
  }

  /** Pending / dead counts and oldest pending event per destination. */
  @GetMapping("/stats")
  public List<DestinationStats> stats() {
    return repository.stats();
  }

  /** Dead-lettered events, keyset-paginated: pass the last {@code id} you saw as {@code after}. */
  @GetMapping("/dead-events")
  public List<DeadEvent> deadEvents(
      @RequestParam(required = false) String destination,
      @RequestParam(defaultValue = "0") long after,
      @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
    return repository.deadEvents(destination, after, limit);
  }

  @PostMapping("/dead-events/replay")
  public Result replay(@Valid @RequestBody EventIds request) {
    return new Result(repository.replay(request.eventIds()));
  }

  @PostMapping("/dead-events/discard")
  public Result discard(@Valid @RequestBody EventIds request) {
    return new Result(repository.discard(request.eventIds()));
  }

  public record EventIds(@NotEmpty @Size(max = 1000) List<UUID> eventIds) {}

  public record Result(int affected) {}
}
