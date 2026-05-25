package raf.edu.week5;

import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 5 - backpressure strategije.
 *
 * Backpressure je protokol kojim consumer kaže producer-u koliko može
 * da podnese. Kad producer ne sme/ne može da uspori, koristimo
 * onBackpressureXxx operatore da odlučimo šta da radimo sa viškom:
 *
 *   onBackpressureBuffer  - čuvaj sve (risk: OOM)
 *   onBackpressureDrop    - baci nove dok consumer ne stigne
 *   onBackpressureLatest  - čuvaj samo POSLEDNJI
 *   onBackpressureError   - baci MissingBackpressureException
 *
 * Demo simulira BRZOG producer-a (1ms interval) i SPOROG consumer-a
 * (50ms obrada po elementu), pa se jasno vidi razlika između strategija.
 *
 * VAŽNO: limitiramo tok preko take() jer su svi producer-i ovde
 * beskonačni - bez take-a bi blockLast nikad ne završio.
 */
public class BackpressureStrategiesDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. DROP - brzi producer, spori consumer, gubi se ===\n");
        primer1_drop();

        System.out.println("\n=== 2. LATEST - vidimo samo poslednje stanje između čitanja ===\n");
        primer2_latest();

        System.out.println("\n=== 3. BUFFER (unbounded) - sve stigne, ali kasno ===\n");
        primer3_bufferUnbounded();

        System.out.println("\n=== 4. BUFFER (bounded) - cap + overflow strategija ===\n");
        primer4_bufferBounded();

        System.out.println("\n=== 5. ERROR - tok pada čim je downstream prespor ===\n");
        primer5_error();

        System.out.println("\n=== 6. limitRate - mi kontrolišemo request size ===\n");
        primer6_limitRate();
    }

    // -------------------------------------------------------------------
    // DROP: kad consumer ne traži (zauzet je), nove vrednosti se bacaju.
    //
    // Primetićemo da consumer NE vidi sve brojeve, već skače - 0, 1, ..., 50, ...
    // Skok je proporcionalan koliko je consumer kasnio.
    //
    // Tipičan slučaj: metrike, telemetrija, klikovi - gde je individualni
    // gubitak prihvatljiv.
    // -------------------------------------------------------------------
    static void primer1_drop() {
        AtomicInteger bacenih = new AtomicInteger();

        Flux.interval(Duration.ofMillis(1))                  // brzi izvor
                .take(200)
                .onBackpressureDrop(dropped -> bacenih.incrementAndGet())
                .publishOn(Schedulers.parallel(), /*prefetch=*/ 1)
                .doOnNext(v -> sporaObrada(50))               // 50ms po elementu
                .doOnNext(v -> log("drop-consumer", v))
                .blockLast();

        log("info", "ukupno bačenih: " + bacenih.get());
    }

    // -------------------------------------------------------------------
    // LATEST: čuva se samo POSLEDNJI element koji je producer emitovao.
    // Kad consumer postane spreman, dobija najsvežiju vrednost.
    //
    // Primetićemo niz tipa: 0, neki srednji broj, neki kasniji srednji, ...
    // Praktično se vidi samo "trenutno stanje" producer-a.
    //
    // Tipičan slučaj: UI stanje, "trenutna pozicija miša", "trenutna cena
    // akcije" - prošlost ne zanima.
    // -------------------------------------------------------------------
    static void primer2_latest() {
        Flux.interval(Duration.ofMillis(1))
                .take(200)
                .onBackpressureLatest()
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sporaObrada(50))
                .doOnNext(v -> log("latest-consumer", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // BUFFER (default, unbounded): producer emituje, sve se gomila u
    // memoriji, consumer čita FIFO redom.
    //
    // Primetićemo: 0, 1, 2, 3, ... bez gubitka, ali consumer kasni
    // ~50ms po elementu. Sa puno elemenata buffer raste.
    //
    // U produkciji NIKAD ne koristiti onBackpressureBuffer() bez
    // limita - OOM je samo pitanje vremena ako consumer ne stigne.
    // -------------------------------------------------------------------
    static void primer3_bufferUnbounded() {
        Flux.interval(Duration.ofMillis(1))
                .take(50)                                     // mali broj, da ne čekamo
                .onBackpressureBuffer()                       // unbounded - opasno!
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sporaObrada(50))
                .doOnNext(v -> log("buffer-consumer", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // BUFFER (bounded): max 10 elemenata u buffer-u. Kad je pun,
    // primenjuje OverflowStrategy:
    //   DROP_LATEST   - baci nove (default)
    //   DROP_OLDEST   - baci najstarije iz buffera
    //   ERROR         - baci exception (tok pada)
    //
    // Ovo je PRAKTIČNA opcija - imamo cap, znamo gornju granicu
    // memorije, i kontrolišemo šta se događa sa viškom.
    // -------------------------------------------------------------------
    static void primer4_bufferBounded() {
        AtomicInteger bacenih = new AtomicInteger();

        Flux.interval(Duration.ofMillis(1))
                .take(200)
                .onBackpressureBuffer(
                        /*maxSize=*/    10,
                        /*onOverflow=*/ dropped -> bacenih.incrementAndGet(),
                        BufferOverflowStrategy.DROP_OLDEST)
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sporaObrada(50))
                .doOnNext(v -> log("bounded-consumer", v))
                .blockLast();

        log("info", "preko bafera od 10, izbačeno: " + bacenih.get());
    }

    // -------------------------------------------------------------------
    // ERROR: bilo kakav backpressure mismatch ruši tok sa
    // reactor.core.Exceptions$OverflowException.
    //
    // Striktan ugovor: "ja sam producer i moj request je poštovan ili
    // ne pričamo". Korisno kad gubitak NIJE prihvatljiv, a ni
    // beskonačan buffer.
    //
    // Hvatamo grešku u onError handler-u da demo ne ispadne ružan.
    // -------------------------------------------------------------------
    static void primer5_error() {
        Flux.interval(Duration.ofMillis(1))
                .take(200)
                .onBackpressureError()
                .publishOn(Schedulers.parallel(), 1)
                .doOnNext(v -> sporaObrada(50))
                .doOnNext(v -> log("error-consumer", v))
                .onErrorResume(ex -> {
                    log("error-resume", ex.getClass().getSimpleName() + ": " + ex.getMessage());
                    return Flux.empty();
                })
                .blockLast();
    }

    // -------------------------------------------------------------------
    // limitRate: nije onBackpressureXxx, ali rešava isti problem sa
    // suprotne strane.
    //
    // Umesto da producer odluči šta da radi sa viškom, MI sa consumer
    // strane kažemo: "tražim po N odjednom, ne više". To se prevodi u
    // request(N) prema upstream-u.
    //
    // Default lowTide je 75%: kad pojedeš 75% od N, traži se još N.
    // Buffer ostaje konstantnog reda veličine N.
    // -------------------------------------------------------------------
    static void primer6_limitRate() {
        Flux.range(1, 20)
                .doOnRequest(n -> log("upstream-request", n))    // vidimo request size
                .limitRate(5)                                     // traži po 5
                .doOnNext(v -> sporaObrada(10))
                .doOnNext(v -> log("limited-consumer", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Helper - simulira blocking obradu. Ne smemo .delayElement jer to
    // unosi async prebacivanje i menja semantiku backpressure-a.
    // -------------------------------------------------------------------
    static void sporaObrada(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-20s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
