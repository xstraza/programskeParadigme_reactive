package raf.edu.week6.ratelimit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 6 - rate limiting / throttling.
 * <p>
 * Spoljni API ima limit (npr. "max 5 zahteva u sekundi", ili "max 3
 * istovremena"). Ako pucamo brže, dobijamo 429 Too Many Requests.
 * Reactor nudi nekoliko poluga da uskladimo brzinu PRODUKCIJE sa
 * dozvoljenom brzinom POTROŠNJE:
 * <p>
 * 1. delayElements(d)   - razmak d između svaka dva elementa
 *                         (fiksna stopa, najprostije).
 * 2. flatMap(fn, N)     - najviše N istovremenih in-flight poziva
 *                         (ograničenje KONKURENTNOSTI, ne stope).
 * 3. zipWith(interval)  - "token bucket": pusti tačno jedan element po
 *                         tiku tajmera (precizna stopa req/s).
 * 4. limitRate(N)       - koliko elemenata operator traži unapred
 *                         (prefetch / backpressure granica).
 */
public class RateLimitDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. delayElements - fiksna stopa (1 svakih 200ms) ===\n");
        primer1_delayElements();

        System.out.println("\n=== 2. flatMap(fn, 3) - najviše 3 istovremena poziva ===\n");
        primer2_konkurentnost();

        System.out.println("\n=== 3. zipWith(interval) - token bucket, ~5 req/s ===\n");
        primer3_tokenBucket();

        System.out.println("\n=== 4. limitRate - prefetch granica ka brzom izvoru ===\n");
        primer4_limitRate();
    }

    // -------------------------------------------------------------------
    // Simulira jedan API poziv: traje 100ms, vraća "ok-<id>".
    // -------------------------------------------------------------------
    static Mono<String> pozovi(int id) {
        return Mono.just("ok-" + id)
                .delayElement(Duration.ofMillis(100))
                .doOnSubscribe(s -> log("->API", "start #" + id));
    }

    // -------------------------------------------------------------------
    // delayElements(200ms): ma koliko brzo izvor proizvodi, ovaj
    // operator ubaci 200ms pauze između elemenata. Stopa = 1 / 200ms
    // = 5 elemenata/s. Najjednostavniji throttle za "ne brže od X".
    // -------------------------------------------------------------------
    static void primer1_delayElements() throws InterruptedException {
        Flux.range(1, 5)
                .delayElements(Duration.ofMillis(200))  // razmak između emisija
                .subscribe(n -> log("emit", n));
        Thread.sleep(1300);
    }

    // -------------------------------------------------------------------
    // flatMap(fn, 3): pokreni pozive, ali drži najviše 3 ISTOVREMENO
    // u letu. Kad se jedan završi, pušta sledeći. Ovo je ograničenje
    // konkurentnosti (koliko paralelno), ne stope (koliko po sekundi).
    //
    // 6 poziva po 100ms sa limitom 3 => dva "talasa" => ~200ms ukupno,
    // a server nikad ne vidi više od 3 odjednom.
    // -------------------------------------------------------------------
    static void primer2_konkurentnost() {
        long start = System.currentTimeMillis();
        List<String> rez = Flux.range(1, 6)
                .flatMap(RateLimitDemo::pozovi, 3)      // concurrency = 3
                .collectList()
                .block();
        log("gotovo", rez + " za ~" + (System.currentTimeMillis() - start) + "ms");
    }

    // -------------------------------------------------------------------
    // Token bucket preko zipWith(interval): zip uparuje i-ti element sa
    // i-tim tikom tajmera. Tajmer kuca svakih 200ms => element izlazi
    // tek kad stigne njegov "token" => tačno 5 req/s, bez obzira koliko
    // brzo izvor proizvodi. Preciznije od delayElements kad izvor već
    // ima ugrađene pauze.
    // -------------------------------------------------------------------
    static void primer3_tokenBucket() throws InterruptedException {
        Flux<Long> tokeni = Flux.interval(Duration.ofMillis(200));  // 5 tokena/s

        Flux.range(1, 5)
                .zipWith(tokeni, (req, token) -> req)   // čekaj token pre emisije
                .subscribe(n -> log("zahtev", n));
        Thread.sleep(1300);
    }

    // -------------------------------------------------------------------
    // limitRate(N): koliko elemenata operator zatraži od uzvodnog
    // izvora unapred (prefetch). Sa brzim izvorom (Flux.range) i sporim
    // potrošačem, limitRate(2) drži "u letu" najviše ~2 elementa umesto
    // da povuče svih 1000 u memoriju. Ovo je čisti backpressure tuning
    // (tema iz nedelje 5), ali u praksi ide ruku pod ruku sa rate
    // limiting-om.
    // -------------------------------------------------------------------
    static void primer4_limitRate() {
        Flux.range(1, 10)
                .doOnRequest(r -> log("request", "izvor dobija zahtev za: " + r))
                .limitRate(2)                           // traži po 2 od izvora
                .map(n -> n * 10)
                .blockLast();
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
