package com.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.r2dbc.DataR2dbcTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@DataR2dbcTest
@Testcontainers
class ApplicationExpiryJobTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine"); // match prod's major version

    @DynamicPropertySource
    static void r2dbcProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
                postgres.getHost(), postgres.getMappedPort(5432), postgres.getDatabaseName()));
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
    }

    @Autowired ApplicationRepository applicationRepository;
    @Autowired DatabaseClient db;

    // Remove this if Flyway/Liquibase already builds the schema in tests.
    @BeforeAll
    static void createSchema(@Autowired DatabaseClient db) {
        db.sql("CREATE SCHEMA obs_submission").then().block();
        db.sql("""
            CREATE TYPE obs_submission.application_status_enum AS ENUM
                ('draft', 'submitted', 'waiting_for_approval', 'approved', 'failed', 'cancelled')
            """).then().block();
        db.sql("""
            CREATE TABLE application_tt (
                id     BIGINT PRIMARY KEY,
                status obs_submission.application_status_enum NOT NULL
            )
            """).then().block();
    }

    // 1. The SQL, against real Postgres. 'approved' is the control row that must stay untouched.
    @Test
    void expiresOnlyPendingApplications() {
        db.sql("""
            INSERT INTO application_tt (id, status) VALUES
                (1, 'draft'), (2, 'submitted'), (3, 'waiting_for_approval'), (4, 'approved')
            """).then().block();

        StepVerifier.create(applicationRepository.expirePending())
                .expectNext(3L)
                .verifyComplete();

        StepVerifier.create(db.sql("SELECT status::text FROM application_tt ORDER BY id")
                        .map(row -> row.get(0, String.class))
                        .all())
                .expectNext("failed", "failed", "cancelled", "approved")
                .verifyComplete();
    }

    // 2. The job. Spring calls it once and re-subscribes every night, so each subscription must rerun the update.
    //    For the void + block() version: call expirePendingApplications() directly, then verify(repo).expirePending().
    @Test
    void runsTheUpdateOnEverySubscription() {
        ApplicationRepository repo = mock(ApplicationRepository.class);
        when(repo.expirePending()).thenReturn(Mono.just(3L));

        Mono<Void> scheduled = new ApplicationExpiryJob(repo).expirePendingApplications();

        StepVerifier.create(scheduled).verifyComplete(); // night 1
        StepVerifier.create(scheduled).verifyComplete(); // night 2

        verify(repo, times(2)).expirePending();
    }

    // 3. The schedule: 23:30 Jakarta time.
    @Test
    void firesAt2330Jakarta() throws Exception {
        Scheduled scheduled = ApplicationExpiryJob.class
                .getMethod("expirePendingApplications")
                .getAnnotation(Scheduled.class);

        ZoneId zone = ZoneId.of(scheduled.zone());
        ZonedDateTime next = CronExpression.parse(scheduled.cron())
                .next(ZonedDateTime.of(2026, 11, 9, 12, 0, 0, 0, zone));

        assertThat(zone).isEqualTo(ZoneId.of("Asia/Jakarta"));
        assertThat(next.toLocalDateTime()).isEqualTo(LocalDateTime.of(2026, 11, 9, 23, 30));
    }
}