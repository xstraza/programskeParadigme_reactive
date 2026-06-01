package raf.edu.week5;

import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 5 - rešenja praktičnih zadataka.
 *
 * Za svaki zadatak iz {@link PracticeTasksForStudents}, ovde je gotov
 * primer koji se može pokrenuti pojedinačno iz main-a.
 */
public class PracticeTasksSolutions {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        // Otkomentariši zadatak na kome radiš.

        // zadatak1();
        // zadatak2();
        // zadatak3();
        // zadatak4();
        // zadatak5();
        // zadatak6();
        // zadatak7();
        // zadatak8();
        // zadatak9();
        // zadatak10();
    }

    // ===================================================================
    // Zadatak 1 - onBackpressureDrop
    // ===================================================================
    static void zadatak1() {
        AtomicInteger bacenih = new AtomicInteger();
        Flux.interval(Duration.ofMillis(1))
                .take(100)
                .onBackpressureDrop(d -> bacenih.incrementAndGet())
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sleep(30))
                .doOnNext(v -> log("consumer", v))
                .blockLast();
        log("info", "bačeno: " + bacenih.get());
    }

    // ===================================================================
    // Zadatak 2 - bounded buffer
    // ===================================================================
    static void zadatak2() {
        AtomicInteger bachenih = new AtomicInteger();
        AtomicInteger obradjenih = new AtomicInteger();

        Flux.interval(Duration.ofMillis(1))
                .take(200)
                .onBackpressureBuffer(
                        /*maxSize=*/    5,
                        /*onOverflow=*/ d -> bachenih.incrementAndGet(),
                        BufferOverflowStrategy.DROP_OLDEST)
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sleep(30))
                .doOnNext(v -> obradjenih.incrementAndGet())
                .blockLast();

        log("obrada", "obradjeno=" + obradjenih.get() + ", bačeno=" + bachenih.get()
                + ", zbir=" + (obradjenih.get() + bachenih.get()));
    }

    // ===================================================================
    // Zadatak 3 - limitRate
    // ===================================================================
    static void zadatak3() {
        Flux.range(1, 100)
                .doOnRequest(n -> log("upstream-request", n))
                .limitRate(10)
                .doOnNext(v -> sleep(5))
                .doOnNext(v -> log("consumer", v))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 4 - onErrorReturn
    // ===================================================================
    static void zadatak4() {
        String rez = Mono.<String>error(new RuntimeException("pao"))
                .onErrorReturn("fallback")
                .block();
        log("rezultat", rez);
    }

    // ===================================================================
    // Zadatak 5 - cascading fallback
    // ===================================================================
    static void zadatak5() {
        Mono<String> primary = Mono.<String>error(new RuntimeException("primary-fail"))
                .doOnSubscribe(s -> log("call", "primary"));
        Mono<String> cache = Mono.<String>error(new RuntimeException("cache-fail"))
                .doOnSubscribe(s -> log("call", "cache"));
        Mono<String> def = Mono.just("default-val")
                .doOnSubscribe(s -> log("call", "default"));

        String rez = primary
                .onErrorResume(ex -> {
                    log("fallback-1", "primary pao: " + ex.getMessage());
                    return cache;
                })
                .onErrorResume(ex -> {
                    log("fallback-2", "cache pao: " + ex.getMessage());
                    return def;
                })
                .block();

        log("rezultat", rez);
    }

    // ===================================================================
    // Zadatak 6 - skip on error
    // ===================================================================
    static void zadatak6() {
        Flux.range(1, 10)
                .flatMap(n -> Mono.fromCallable(() -> {
                            if (n % 2 == 0) throw new RuntimeException("paran " + n);
                            return n;
                        })
                        .onErrorResume(ex -> {
                            log("preskoči", n);
                            return Mono.empty();
                        }))
                .subscribe(v -> log("out", v));
    }

    // ===================================================================
    // Zadatak 7 - retry(n)
    // ===================================================================
    static void zadatak7() {
        AtomicInteger p = new AtomicInteger();
        String rez = flakyServis(p, 4)
                .retry(3)
                .block();
        log("rezultat", "uspeh, pokušaja=" + p.get() + " rez=" + rez);
    }

    // ===================================================================
    // Zadatak 8 - Retry.backoff sa jitter-om
    // ===================================================================
    static void zadatak8() {
        AtomicInteger p = new AtomicInteger();
        long t0 = System.currentTimeMillis();

        String rez = flakyServis(p, 4)
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
                        .jitter(0.5)
                        .doBeforeRetry(rs -> log("retry",
                                "pokušaj " + (rs.totalRetries() + 2)
                                        + " @ " + (System.currentTimeMillis() - t0) + "ms")))
                .block();

        log("rezultat", "rez=" + rez + ", ukupno=" + (System.currentTimeMillis() - t0) + "ms");
    }

    // ===================================================================
    // Zadatak 9 - retry samo za određene greške
    // ===================================================================
    static void zadatak9() {
        Retry policy = Retry.backoff(3, Duration.ofMillis(100))
                .filter(ex -> ex instanceof IOException)
                .doBeforeRetry(rs -> log("retry", "razlog: " + rs.failure().getClass().getSimpleName()));

        log("info", "(a) IOException - retry uspeva:");
        AtomicInteger p1 = new AtomicInteger();
        try {
            Mono.fromCallable(() -> {
                        int n = p1.incrementAndGet();
                        log("a-servis", "pokušaj " + n);
                        if (n < 3) throw new IOException("net down");
                        return "ok";
                    })
                    .retryWhen(policy)
                    .block();
            log("a-rezultat", "uspeh, pokušaja=" + p1.get());
        } catch (Exception ex) {
            log("a-rezultat", "PAO: " + ex.getMessage());
        }

        log("info", "(b) IllegalArgumentException - filter ga odbacuje, odmah pada:");
        AtomicInteger p2 = new AtomicInteger();
        try {
            Mono.fromCallable(() -> {
                        int n = p2.incrementAndGet();
                        log("b-servis", "pokušaj " + n);
                        throw new IllegalArgumentException("bug u kodu");
                    })
                    .retryWhen(policy)
                    .block();
        } catch (Exception ex) {
            log("b-rezultat", "PAO posle " + p2.get() + " pokušaja: "
                    + ex.getClass().getSimpleName());
        }
    }

    // ===================================================================
    // Zadatak 10 - kompletna resilience strategija
    // ===================================================================
    static void zadatak10() {
        AtomicInteger fallbackCount = new AtomicInteger();

        for (int i = 1; i <= 5; i++) {
            String rez = httpPoziv()
                    .timeout(Duration.ofMillis(200))
                    .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                            .jitter(0.5)
                            .filter(ex -> ex instanceof TimeoutException
                                       || ex instanceof IOException))
                    .onErrorReturn("FALLBACK")
                    .block();

            if ("FALLBACK".equals(rez)) fallbackCount.incrementAndGet();
            log("poziv-" + i, rez);
        }

        log("statistika", "fallback-ova: " + fallbackCount.get() + "/5");
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /** Servis koji uspeva tek na n-tom pokušaju. */
    static Mono<String> flakyServis(AtomicInteger pokusaji, int uspevaNa) {
        return Mono.fromCallable(() -> {
            int n = pokusaji.incrementAndGet();
            log("servis", "pokušaj " + n);
            if (n < uspevaNa) throw new IOException("flaky #" + n);
            return "data@" + n;
        });
    }

    /**
     * Simulira HTTP poziv - random trajanje (~50-150ms) i 70% verovatnoća
     * pada. Random je dovoljno "loš" da se vidi rad retry/fallback-a.
     */
    static Mono<String> httpPoziv() {
        return Mono.fromCallable(() -> {
                    long delay = 50 + (long) (Math.random() * 100);
                    Thread.sleep(delay);
                    if (Math.random() < 0.7) {
                        throw new IOException("simulirana mrežna greška");
                    }
                    return "ok@" + delay + "ms";
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
