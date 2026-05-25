package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 4 - rešenja praktičnih zadataka.
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
    // Zadatak 1 - vidi niti
    // ===================================================================
    static void zadatak1() {
        log("info", "BEZ subscribeOn-a:");
        Flux.range(1, 5)
                .doOnNext(n -> log("default", n))            // main
                .blockLast();

        log("info", "SA subscribeOn(boundedElastic):");
        Flux.range(1, 5)
                .doOnNext(n -> log("elastic", n))            // boundedElastic-X
                .subscribeOn(Schedulers.boundedElastic())
                .blockLast();
    }

    // ===================================================================
    // Zadatak 2 - subscribeOn na različitim mestima
    //
    // Sve tri varijante daju isti rezultat: ceo lanac na boundedElastic.
    // Mesto u lancu ne utiče - subscribe putuje od dna ka vrhu i
    // pri nailasku na subscribeOn postavlja nit za ceo upstream.
    // ===================================================================
    static void zadatak2() {
        log("info", "(a) subscribeOn NA POČETKU:");
        Flux.range(1, 3)
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(n -> log("a-source", n))
                .map(n -> n * 10)
                .doOnNext(n -> log("a-mapped", n))
                .blockLast();

        log("info", "(b) subscribeOn NA SREDINI:");
        Flux.range(1, 3)
                .doOnNext(n -> log("b-source", n))
                .subscribeOn(Schedulers.boundedElastic())
                .map(n -> n * 10)
                .doOnNext(n -> log("b-mapped", n))
                .blockLast();

        log("info", "(c) subscribeOn NA KRAJU:");
        Flux.range(1, 3)
                .doOnNext(n -> log("c-source", n))
                .map(n -> n * 10)
                .doOnNext(n -> log("c-mapped", n))
                .subscribeOn(Schedulers.boundedElastic())
                .blockLast();
    }

    // ===================================================================
    // Zadatak 3 - publishOn deli lanac
    // ===================================================================
    static void zadatak3() {
        Flux.range(1, 3)
                .doOnNext(n -> log("segment-A", n))           // main
                .publishOn(Schedulers.parallel())
                .doOnNext(n -> log("segment-B", n))           // parallel-X
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(n -> log("segment-C", n))           // boundedElastic-X
                .blockLast();
    }

    // ===================================================================
    // Zadatak 4 - blocking poziv pravilno spakovan
    // ===================================================================
    static String blockingFetch(int id) {
        log("blocking", "fetch " + id);
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        return "data-" + id;
    }

    static void zadatak4() {
        log("info", "jedan blocking poziv:");
        Mono.fromCallable(() -> blockingFetch(101))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(v -> log("primio", v))
                .block();

        log("info", "5 paralelnih blocking poziva:");
        long t0 = System.currentTimeMillis();
        Flux.range(101, 5)
                .flatMap(id -> Mono.fromCallable(() -> blockingFetch(id))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnNext(v -> log("primio", v))
                .blockLast();
        log("info", "ukupno trajalo: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // ===================================================================
    // Zadatak 5 - delayElements i implicitna nit
    // ===================================================================
    static void zadatak5() {
        Flux.just("A", "B", "C")
                .doOnNext(v -> log("pre", v))                 // main
                .delayElements(Duration.ofMillis(100))
                .doOnNext(v -> log("post", v))                // parallel-X
                .blockLast();

        log("info", "Sa custom scheduler-om za delay:");
        Flux.just("A", "B", "C")
                .doOnNext(v -> log("pre", v))                 // main
                .delayElements(Duration.ofMillis(100), Schedulers.single())
                .doOnNext(v -> log("post-single", v))         // single-X
                .blockLast();
    }

    // ===================================================================
    // Zadatak 6 - polling pattern (interval + blocking)
    // ===================================================================
    static String blockingFetchStatus(long tick) {
        log("blocking", "poll @" + tick);
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
        return "status@" + tick;
    }

    static void zadatak6() {
        log("info", "polling sa flatMap (paralelno ako poll traje duže od interval-a):");
        Flux.interval(Duration.ofMillis(400))
                .take(3)
                .flatMap(tick -> Mono.fromCallable(() -> blockingFetchStatus(tick))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnNext(s -> log("status", s))
                .blockLast();

        log("info", "polling sa concatMap (strogo serijski):");
        Flux.interval(Duration.ofMillis(400))
                .take(3)
                .concatMap(tick -> Mono.fromCallable(() -> blockingFetchStatus(tick))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnNext(s -> log("status", s))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 7 - timeout sa fallback Publisher-om
    // ===================================================================
    static String blockingFetchNamed(String name, long ms) {
        log("blocking", "fetch " + name);
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
        return name;
    }

    static void zadatak7() {
        log("info", "primary spor (500ms) > timeout (200ms) -> ide cache:");
        Mono<String> primarni = Mono.fromCallable(() -> blockingFetchNamed("primary", 500))
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> cache = Mono.fromCallable(() -> blockingFetchNamed("cache", 50))
                .subscribeOn(Schedulers.boundedElastic());

        String rez1 = primarni
                .timeout(Duration.ofMillis(200), cache)
                .block();
        log("rezultat", rez1);    // očekivano "cache"

        log("info", "primary brz (100ms) < timeout (200ms) -> ide primary:");
        Mono<String> primarniBrz = Mono.fromCallable(() -> blockingFetchNamed("primary", 100))
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> cache2 = Mono.fromCallable(() -> blockingFetchNamed("cache", 50))
                .subscribeOn(Schedulers.boundedElastic());

        String rez2 = primarniBrz
                .timeout(Duration.ofMillis(200), cache2)
                .block();
        log("rezultat", rez2);    // očekivano "primary"
    }

    // ===================================================================
    // Zadatak 8 - CPU heavy posao paralelno
    // ===================================================================
    static int slowSquare(int n) {
        long end = System.nanoTime() + Duration.ofMillis(150).toNanos();
        int x = n;
        while (System.nanoTime() < end) {
            x = (x * 1103515245 + 12345) & 0x7FFFFFFF;        // pseudo CPU rad
        }
        return n * n;
    }

    static void zadatak8() {
        log("info", "sekvencijalno:");
        long t0 = System.currentTimeMillis();
        Flux.range(1, 8)
                .map(PracticeTasksSolutions::slowSquare)
                .blockLast();
        log("info", "sekvencijalno: " + (System.currentTimeMillis() - t0) + "ms");

        log("info", "paralelno (4 rail-a):");
        long t1 = System.currentTimeMillis();
        Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(PracticeTasksSolutions::slowSquare)
                .sequential()
                .blockLast();
        log("info", "paralelno:     " + (System.currentTimeMillis() - t1) + "ms");
    }

    // ===================================================================
    // Zadatak 9 - zip paralelnih blocking poziva
    // ===================================================================
    static String blockingProfile() {
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        return "profile";
    }

    static String blockingPosts() {
        try { Thread.sleep(300); } catch (InterruptedException ignored) {}
        return "posts";
    }

    static String blockingFriends() {
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        return "friends";
    }

    static void zadatak9() {
        long t0 = System.currentTimeMillis();

        Mono<String> p = Mono.fromCallable(PracticeTasksSolutions::blockingProfile)
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> po = Mono.fromCallable(PracticeTasksSolutions::blockingPosts)
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> f = Mono.fromCallable(PracticeTasksSolutions::blockingFriends)
                .subscribeOn(Schedulers.boundedElastic());

        String dashboard = Mono.zip(p, po, f)
                .map(t -> "Dashboard: " + t.getT1() + " + " + t.getT2() + " + " + t.getT3())
                .block();

        log("dashboard", dashboard);
        log("info", "ukupno: " + (System.currentTimeMillis() - t0) + "ms (oko 500ms = max)");
    }

    // ===================================================================
    // Zadatak 10 - kombinovan pipeline (I/O + CPU)
    // ===================================================================
    static String cpuHeavyTransform(String s) {
        long end = System.nanoTime() + Duration.ofMillis(50).toNanos();
        int x = s.hashCode();
        while (System.nanoTime() < end) {
            x = (x * 1103515245 + 12345) & 0x7FFFFFFF;
        }
        return s.toUpperCase() + "#" + x;
    }

    static void zadatak10() {
        List<String> rez = Flux.range(1, 10)
                .flatMap(id -> Mono.fromCallable(() -> {
                                    log("io-fetch", id);
                                    return blockingFetch(id);
                                })
                                .subscribeOn(Schedulers.boundedElastic()),
                        /* concurrency = */ 3)
                .publishOn(Schedulers.parallel())
                .map(s -> {
                    log("cpu-transform", s);
                    return cpuHeavyTransform(s);
                })
                .collectList()
                .block();

        log("info", "rezultat (" + (rez == null ? 0 : rez.size()) + " elemenata): " + rez);
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
