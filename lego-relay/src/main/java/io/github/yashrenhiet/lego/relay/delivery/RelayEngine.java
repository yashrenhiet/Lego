package io.github.yashrenhiet.lego.relay.delivery;

import io.github.yashrenhiet.lego.relay.lease.Lease;
import io.github.yashrenhiet.lego.relay.lease.PartitionLeaseManager;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Drives the relay.
 *
 * <ul>
 *   <li>A lease thread runs {@link PartitionLeaseManager#tick()} every renew interval.
 *   <li>A dispatcher thread wakes every poll interval and hands each leased partition that is not
 *       already being processed to the worker pool. The {@code inFlight} set guarantees a
 *       partition is never processed by two threads at once, which is what keeps per-key order.
 * </ul>
 */
public class RelayEngine implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(RelayEngine.class);

  private final PartitionLeaseManager leases;
  private final PartitionProcessor processor;
  private final Duration pollInterval;
  private final Duration renewInterval;
  private final int workerThreads;
  private final int maxConsecutiveBatches;
  private final Duration shutdownTimeout;
  private final Set<Integer> inFlight = ConcurrentHashMap.newKeySet();

  private volatile boolean running;
  private ScheduledExecutorService scheduler;
  private ExecutorService workers;

  public RelayEngine(
      PartitionLeaseManager leases,
      PartitionProcessor processor,
      Duration pollInterval,
      Duration renewInterval,
      int workerThreads,
      int maxConsecutiveBatches,
      Duration shutdownTimeout) {
    this.leases = leases;
    this.processor = processor;
    this.pollInterval = pollInterval;
    this.renewInterval = renewInterval;
    this.workerThreads = workerThreads;
    this.maxConsecutiveBatches = maxConsecutiveBatches;
    this.shutdownTimeout = shutdownTimeout;
  }

  @Override
  public synchronized void start() {
    if (running) {
      return;
    }
    scheduler = Executors.newScheduledThreadPool(2, Thread.ofPlatform().name("lego-scheduler-", 0).factory());
    workers = Executors.newFixedThreadPool(workerThreads, Thread.ofPlatform().name("lego-worker-", 0).factory());
    running = true;
    scheduler.scheduleWithFixedDelay(this::safeTick, 0, renewInterval.toMillis(), TimeUnit.MILLISECONDS);
    scheduler.scheduleWithFixedDelay(this::dispatch, 0, pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    log.info("Lego relay started as '{}' with {} worker threads", leases.instanceId(), workerThreads);
  }

  @Override
  public synchronized void stop() {
    if (!running) {
      return;
    }
    running = false;
    scheduler.shutdownNow();
    workers.shutdown();
    try {
      if (!workers.awaitTermination(shutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        workers.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      workers.shutdownNow();
    }
    leases.releaseAll();
    log.info("Lego relay stopped");
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private void safeTick() {
    try {
      leases.tick();
    } catch (RuntimeException e) {
      // Deliberately broad: an exception escaping a scheduled task silently cancels it forever.
      log.error("Lease maintenance failed; will retry", e);
    }
  }

  private void dispatch() {
    try {
      for (Lease lease : leases.activeLeases()) {
        if (running && inFlight.add(lease.partition())) {
          workers.execute(() -> drain(lease));
        }
      }
    } catch (RuntimeException e) {
      // Deliberately broad: see safeTick().
      log.error("Dispatch failed; will retry", e);
    }
  }

  private void drain(Lease lease) {
    try {
      for (int i = 0; i < maxConsecutiveBatches && running && leases.stillHolds(lease); i++) {
        if (!processor.processBatch(lease)) {
          return;
        }
      }
    } catch (RuntimeException e) {
      // Deliberately broad: one bad partition must not take the worker thread down.
      log.error("Processing partition {} failed; will retry", lease.partition(), e);
    } finally {
      inFlight.remove(lease.partition());
    }
  }
}
