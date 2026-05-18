package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 4 - kako spakovati BLOCKING poziv u reaktivni tok.
 *
 * Realnost projekta: imamo stari kod koji koristi JDBC, FileInputStream,
 * legacy SDK, javax.mail, ... - sve to BLOKIRA nit. Reactor ne sme
 * da bude na default (parallel) pool-u dok takav poziv traje, jer
 * parallel ima samo onoliko niti koliko ima jezgara.
 *
 * Rešenje: Schedulers.boundedElastic - pool praviljen za blocking
 * pozive. Ima do 10×cores niti, TTL 60s, vraća se na 0 kad nema posla.
 *
 * Pattern:
 *   Mono.fromCallable(() -> blockingCall())
 *       .subscribeOn(Schedulers.boundedElastic())
 *
 * Ovaj demo simulira blocking pozive (Thread.sleep + log) i pokazuje
 * tipične produkcijske obrasce.
 */
public class BlockingWrapperDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. Goli blocking poziv u Mono ===\n");
        primer1_goliBlocking();

        System.out.println("\n=== 2. Više blocking poziva paralelno preko flatMap ===\n");
        primer2_paralelniBlocking();

        System.out.println("\n=== 3. zip - 3 nezavisna izvora paralelno ===\n");
        primer3_zip();

        System.out.println("\n=== 4. Ograničenje paralelizma - flatMap(.., concurrency) ===\n");
        primer4_ogranicenParalelizam();

        System.out.println("\n=== 5. Šta NE raditi - blocking na default niti ===\n");
        primer5_antipattern();
    }

    // -------------------------------------------------------------------
    // Najjednostavniji pattern: wrapovati blokirajuću sinhronu metodu u
    // Mono.fromCallable i prebaciti subscription na boundedElastic.
    //
    // Sve PRE subscribeOn-a (a to je ovde samo izvor - fromCallable) izvršava
    // na elastic niti. Bez subscribeOn-a, blocking poziv bi tekao na
    // 'main' niti i blokirao ceo program.
    // -------------------------------------------------------------------
    static void primer1_goliBlocking() {
        Mono.fromCallable(() -> blockingFetchUser(101))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(u -> log("primio", u))
                .block();
    }

    // -------------------------------------------------------------------
    // 5 ID-jeva, za svaki blocking poziv. Pomoću flatMap-a koji vraća
    // Mono sa subscribeOn-om, svaki poziv ide na svoju elastic nit -
    // paralelno.
    //
    // Vidi se po imenima niti (boundedElastic-1, -2, -3, ...) da rade
    // istovremeno.
    // -------------------------------------------------------------------
    static void primer2_paralelniBlocking() {
        long t0 = System.currentTimeMillis();

        Flux.range(101, 5)
                .flatMap(id -> Mono.fromCallable(() -> blockingFetchUser(id))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnNext(u -> log("primio", u))
                .blockLast();

        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms (oko 200ms, ne 1000)");
    }

    // -------------------------------------------------------------------
    // Mono.zip - kombinuje 3 nezavisna izvora. Sva tri imaju svoj
    // subscribeOn(elastic), pa rade paralelno.
    //
    // Ukupno trajanje = MAX(profile, posts, friends), ne SUM.
    // -------------------------------------------------------------------
    static void primer3_zip() {
        long t0 = System.currentTimeMillis();

        Mono<String> profile = Mono.fromCallable(() -> blockingFetch("profile", 300))
                .subscribeOn(Schedulers.boundedElastic());

        Mono<String> posts = Mono.fromCallable(() -> blockingFetch("posts", 500))
                .subscribeOn(Schedulers.boundedElastic());

        Mono<String> friends = Mono.fromCallable(() -> blockingFetch("friends", 200))
                .subscribeOn(Schedulers.boundedElastic());

        Mono.zip(profile, posts, friends)
                .map(t -> "Dashboard{" + t.getT1() + " + " + t.getT2() + " + " + t.getT3() + "}")
                .doOnNext(d -> log("dashboard", d))
                .block();

        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms (oko 500ms = max)");
    }

    // -------------------------------------------------------------------
    // U produkciji ne želimo 100 paralelnih JDBC poziva - pool baze ne
    // izdrži. flatMap ima drugi parametar: concurrency.
    //
    // Sa concurrency=3, najviše 3 unutrašnja toka rade istovremeno.
    // -------------------------------------------------------------------
    static void primer4_ogranicenParalelizam() {
        long t0 = System.currentTimeMillis();

        Flux.range(101, 6)
                .flatMap(id -> Mono.fromCallable(() -> blockingFetchUser(id))
                                .subscribeOn(Schedulers.boundedElastic()),
                        /* concurrency = */ 3)
                .doOnNext(u -> log("primio", u))
                .blockLast();

        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms (oko 400ms = 6/3 × 200ms)");
    }

    // -------------------------------------------------------------------
    // ANTI-PATTERN: blocking poziv BEZ subscribeOn-a. Izvršava na 'main'
    // niti (ili koj god je u tom trenutku aktivna). Ako je ovo u
    // produkciji na parallel pool-u, **deli sa svim drugim Mono-ima**
    // - može da blokira ceo sistem.
    //
    // U ovom main-u nema posledica jer je program samostalan, ali u
    // pravom servisu (Spring WebFlux) - ovo obara performanse.
    // -------------------------------------------------------------------
    static void primer5_antipattern() {
        log("info", "BEZ subscribeOn-a:");

        Mono.fromCallable(() -> blockingFetchUser(999))
                .doOnNext(u -> log("primio", u))
                .block();   // izvršava se na 'main', blokira main
    }

    // ===================================================================
    // Simulacije blocking poziva - zapamti, Thread.sleep MOZE da se baci
    // jedino kao InterruptedException, pa ga "gutamo" u demo-u.
    // ===================================================================

    static String blockingFetchUser(int id) {
        log("blocking", "fetch user " + id + " (200ms blocking)");
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        return "User#" + id;
    }

    static String blockingFetch(String name, long ms) {
        log("blocking", "fetch " + name + " (" + ms + "ms blocking)");
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
        return name;
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
