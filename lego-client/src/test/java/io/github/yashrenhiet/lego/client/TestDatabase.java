package io.github.yashrenhiet.lego.client;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/**
 * One embedded PostgreSQL per JVM, migrated with the real schema. No Docker required.
 *
 * <p>Tests call {@link #reset()} to start from a clean outbox.
 */
public final class TestDatabase {

  private static EmbeddedPostgres postgres;

  private TestDatabase() {}

  public static synchronized DataSource dataSource() {
    if (postgres == null) {
      try {
        postgres = EmbeddedPostgres.builder().start();
      } catch (IOException e) {
        throw new UncheckedIOException("Could not start embedded PostgreSQL", e);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(TestDatabase::close));
      Flyway.configure()
          .dataSource(postgres.getPostgresDatabase())
          .locations("classpath:db/lego")
          .load()
          .migrate();
    }
    return postgres.getPostgresDatabase();
  }

  /** JDBC URL of the embedded database, for tests that boot a full Spring context. */
  public static String jdbcUrl() {
    dataSource();
    return postgres.getJdbcUrl("postgres", "postgres");
  }

  public static void reset() {
    try (Connection connection = dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("TRUNCATE lego_outbox, lego_instance RESTART IDENTITY");
      statement.execute(
          "UPDATE lego_partition SET owner = NULL, lease_until = NULL, generation = 0");
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void close() {
    try {
      postgres.close();
    } catch (IOException ignored) {
      // JVM is shutting down.
    }
  }
}
