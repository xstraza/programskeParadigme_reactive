package raf.edu.week5;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 5 - retry pattern u Reactor-u.
 *
 * Retry znači: kad stigne onError, RESUBSCRIBE na isti Publisher.
 * Tok kreće iznova - svi side-effect-i se ponavljaju, svi
 * subscribeOn/publishOn se opet aktiviraju.
 *
 * Varijante:
 *
 *   retry()                              - beskonačno, opasno
 *   retry(n)                             - n pokušaja bez delay-a
 *   retryWhen(Retry.fixedDelay(n, d))    - n pokušaja sa fiksnim delay-em
 *   retryWhen(Retry.backoff(n, d))       - eksponencijalni backoff sa jitter-om
 *   retryWhen(Retry.indefinitely())      - beskonačno, ali sa filter-om
 *
 * Demo koristi simuliran flaky servis koji uspeva tek na N-tom pokušaju.
 * Brojač pokušaja se vidi kroz logove.
 */
public class RetryDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. retry(3) - tri brza pokušaja, pa odustaje ===\n");
        primer1_retry3();

        System.out.println("\n=== 2. retry(5) - dovoljno pokušaja, uspeva ===\n");
        primer2_retry5();

        System.out.println("\n=== 3. Retry.fixedDelay - svaki pokušaj 300ms kasnije ===\n");
        primer3_fixedDelay();

        System.out.println("\n=== 4. Retry.backoff - eksponencijalni delay + jitter ===\n");
        primer4_backoff();

        System.out.println("\n=== 5. Retry.backoff sa filter - samo za određene greške ===\n");
        primer5_filter();

        System.out.println("\n=== 6. RetryExhausted - propagiranje originalne greške ===\n");
        primer6_exhausted();

        System.out.println("\n=== 7. retry + side-effect (idempotency upozorenje) ===\n");
        primer7_sideEffect();
    }

    // -------------------------------------------------------------------
    // retry(3): 1 originalan + 3 retry-a = 4 ukupna pokušaja. Servis
    // uspeva tek na 5. pokušaju, pa retry ISTEKNE i propagira grešku.
    //
    // Bez delay-a - svi pokušaji se izvršavaju brzo jedan za drugim.
    // -------------------------------------------------------------------
    static void primer1_retry3() {
        AtomicInteger pokusaji = new AtomicInteger();
        try {
            flakyServis(pokusaji, 5)
                    .retry(3)
                    .block();
        } catch (Exception ex) {
            log("rezultat", "PROPALO posle " + pokusaji.get() + " pokušaja: " + ex.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // retry(5): 1 + 5 = 6 pokušaja. Servis uspeva na 5., pa retry ima
    // dovoljno prostora i vraća vrednost.
    // -------------------------------------------------------------------
    static void primer2_retry5() {
        AtomicInteger pokusaji = new AtomicInteger();
        String rez = flakyServis(pokusaji, 5)
                .retry(5)
                .block();
        log("rezultat", "USPEH posle " + pokusaji.get() + " pokušaja: " + rez);
    }

    // -------------------------------------------------------------------
    // Retry.fixedDelay: konstantan delay između pokušaja. Korisno kad
    // znamo recovery time upstream-a i ne treba nam eksponencijalno
    // čekanje.
    //
    // 4 pokušaja * 300ms čekanja između = ukupno ~900ms minimum.
    // -------------------------------------------------------------------
    static void primer3_fixedDelay() {
        AtomicInteger pokusaji = new AtomicInteger();
        long t0 = System.currentTimeMillis();

        String rez = flakyServis(pokusaji, 4)
                .retryWhen(Retry.fixedDelay(4, Duration.ofMillis(300)))
                .block();

        log("rezultat", "USPEH posle " + pokusaji.get() + " pokušaja: " + rez);
        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // Retry.backoff(n, firstBackoff) - eksponencijalni backoff:
    //   pokušaj 1: padne
    //   čekaj ~100ms (±jitter)
    //   pokušaj 2: padne
    //   čekaj ~200ms (±jitter)
    //   pokušaj 3: padne
    //   čekaj ~400ms (±jitter)
    //   ...
    //
    // jitter(0.5) dodaje ±50% slučajnosti - sprečava "thundering herd"
    // ako više klijenata istovremeno retry-uje.
    //
    // maxBackoff caps maksimalni delay - posle ovog se ne raste više.
    // -------------------------------------------------------------------
    static void primer4_backoff() {
        AtomicInteger pokusaji = new AtomicInteger();
        long t0 = System.currentTimeMillis();

        String rez = flakyServis(pokusaji, 4)
                .retryWhen(Retry.backoff(5, Duration.ofMillis(100))
                        .jitter(0.5)
                        .maxBackoff(Duration.ofSeconds(2))
                        .doBeforeRetry(rs -> log("backoff",
                                "pokušaj " + (rs.totalRetries() + 2) + " nakon greške: " + rs.failure().getMessage())))
                .block();

        log("rezultat", "USPEH posle " + pokusaji.get() + " pokušaja: " + rez);
        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // .filter(...) - retry SAMO za određene tipove greške. NPE, IAE i
    // slični "bug" exception-i odmah propadaju, bez retry-a.
    //
    // Demo poredi dva slučaja:
    //   (a) IOException - retry, eventualno uspeva
    //   (b) NullPointerException - bez retry-a, odmah pada
    // -------------------------------------------------------------------
    static void primer5_filter() {
        Retry retryPolicy = Retry.backoff(3, Duration.ofMillis(100))
                .filter(ex -> ex instanceof IOException || ex instanceof TimeoutException)
                .doBeforeRetry(rs -> log("retry", "razlog: " + rs.failure().getClass().getSimpleName()));

        log("info", "(a) IOException - retry pomaže:");
        AtomicInteger p1 = new AtomicInteger();
        try {
            servisSaTipom(p1, 3, new IOException("network down"))
                    .retryWhen(retryPolicy)
                    .block();
            log("rezultat-a", "uspeh");
        } catch (Exception ex) {
            log("rezultat-a", "PAO: " + ex.getMessage());
        }

        log("info", "(b) NullPointerException - filter ga odbacuje, odmah pada:");
        AtomicInteger p2 = new AtomicInteger();
        try {
            servisSaTipom(p2, 3, new NullPointerException("bug"))
                    .retryWhen(retryPolicy)
                    .block();
        } catch (Exception ex) {
            log("rezultat-b", "PAO odmah (1 pokušaj=" + p2.get() + "): " + ex.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // Po default-u, kada retry istekne, baca se RetryExhaustedException
    // koji obavija originalni uzrok.
    //
    // Ako želimo originalni exception nepromenjen, koristimo
    // onRetryExhaustedThrow:
    // -------------------------------------------------------------------
    static void primer6_exhausted() {
        log("info", "(a) default - dobijamo RetryExhaustedException:");
        AtomicInteger p1 = new AtomicInteger();
        try {
            flakyServis(p1, 99)
                    .retryWhen(Retry.fixedDelay(2, Duration.ofMillis(50)))
                    .block();
        } catch (Exception ex) {
            log("default-throw", ex.getClass().getSimpleName() + " (cause: "
                    + (ex.getCause() != null ? ex.getCause().getClass().getSimpleName() : "n/a") + ")");
        }

        log("info", "(b) onRetryExhaustedThrow - propagira originalni:");
        AtomicInteger p2 = new AtomicInteger();
        try {
            flakyServis(p2, 99)
                    .retryWhen(Retry.fixedDelay(2, Duration.ofMillis(50))
                            .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                    .block();
        } catch (Exception ex) {
            log("custom-throw", ex.getClass().getSimpleName() + ": " + ex.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // Side-effect demo: retry RESUBSCRIBE-uje. To znači da se SVE iz
    // pipeline-a izvršava ponovo - uključujući bilo kakav side-effect
    // pre tačke gde greška nastaje.
    //
    // Ovde simuliramo "POST request" koji INKREMENTUJE brojač pre nego
    // padne. Posle 4 pokušaja vidimo 4 inkrementa - što znači da bi smo
    // u realnoj produkciji imali 4 dupla POST-a.
    //
    // Rešenje: idempotency keys, ili ne retry-uj non-idempotent operacije.
    // -------------------------------------------------------------------
    static void primer7_sideEffect() {
        AtomicInteger postCount = new AtomicInteger();

        Mono<String> nonIdempotentPost = Mono.fromCallable(() -> {
            int n = postCount.incrementAndGet();
            log("POST", "br. " + n + " - kreirao bi resurs na serveru!");
            if (n < 4) throw new RuntimeException("network");
            return "ok-" + n;
        });

        nonIdempotentPost
                .retryWhen(Retry.fixedDelay(5, Duration.ofMillis(50)))
                .block();

        log("upozorenje",
                "Ukupno POST poziva: " + postCount.get()
                        + " - duplikati na serveru ako nije idempotent!");
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /** Servis koji uspeva tek na n-tom pokušaju. */
    static Mono<String> flakyServis(AtomicInteger pokusaji, int uspevaNa) {
        return Mono.fromCallable(() -> {
            int p = pokusaji.incrementAndGet();
            log("servis", "pokušaj " + p);
            if (p < uspevaNa) throw new IOException("flaky greška #" + p);
            return "data@" + p;
        });
    }

    /** Servis koji uvek baca ZADATI tip exception-a. */
    static Mono<String> servisSaTipom(AtomicInteger pokusaji, int uspevaNa, RuntimeException grueska) {
        return Mono.fromCallable(() -> {
            int p = pokusaji.incrementAndGet();
            log("servis", "pokušaj " + p + " (baca " + grueska.getClass().getSimpleName() + ")");
            if (p < uspevaNa) throw grueska;
            return "data@" + p;
        });
    }

    /** Overload za checked exception varijante. */
    static Mono<String> servisSaTipom(AtomicInteger pokusaji, int uspevaNa, Exception grueska) {
        return Mono.fromCallable(() -> {
            int p = pokusaji.incrementAndGet();
            log("servis", "pokušaj " + p + " (baca " + grueska.getClass().getSimpleName() + ")");
            if (p < uspevaNa) throw grueska;
            return "data@" + p;
        });
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-20s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
