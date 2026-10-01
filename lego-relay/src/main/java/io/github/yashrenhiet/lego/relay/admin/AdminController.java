package io.github.yashrenhiet.lego.relay.admin;

import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DeadEvent;
import io.github.yashrenhiet.lego.relay.admin.AdminRepository.DestinationStats;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read-only operations API. Data-changing endpoints live in {@link DeadLetterCommandController}. */
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
}
