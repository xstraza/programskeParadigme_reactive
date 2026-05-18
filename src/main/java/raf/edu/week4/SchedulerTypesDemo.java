package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 4 - vrste Scheduler-a u Reactor-u.
 *
 * Scheduler je apstrakcija nad pool-om niti. Reactor isporučuje četiri
 * fabriku-metode preko {@link Schedulers}:
 *
 *   Schedulers.parallel()        - fiksan broj niti = broj jezgara,
 *                                  za CPU-bound rad. NE za blocking.
 *
 *   Schedulers.boundedElastic()  - rastući pool (do 10×cores), TTL 60s,
 *                                  za BLOCKING I/O (JDBC, legacy SDK).
 *
 *   Schedulers.single()          - jedna nit, serijski rad.
 *
 *   Schedulers.immediate()       - bez prebacivanja, radi tu gde si.
 *
 * Ovaj demo redom pušta isti tok na svakom scheduler-u i ispisuje
 * ime niti - tako se vidi razlika u praksi.
 */
public class SchedulerTypesDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. Default - bez ičega, ostaje na 'main' ===\n");
        defaultBezScheduler();

        System.out.println("\n=== 2. parallel - CPU pool, fiksan broj niti ===\n");
        with(Schedulers.parallel(), "parallel");

        System.out.println("\n=== 3. boundedElastic - pool za blocking I/O ===\n");
        with(Schedulers.boundedElastic(), "boundedElastic");

        System.out.println("\n=== 4. single - jedna nit, sve serijski ===\n");
        with(Schedulers.single(), "single");

        System.out.println("\n=== 5. immediate - nema prebacivanja niti ===\n");
        with(Schedulers.immediate(), "immediate");

        System.out.println("\n=== 6. parallel ima FIKSAN broj niti, raspodela: ===\n");
        parallelRaspodela();
    }

    // -------------------------------------------------------------------
    // Bez ijednog scheduler-a - sve radi na niti koja pozove subscribe.
    // U main-u to je nit "main".
    //
    // Pravilo: Publisher ne pravi niti sam od sebe. Tek operator koji
    // ima vremensku dimenziju (delayElements, interval) ili eksplicitan
    // subscribeOn/publishOn prebacuje nit.
    // -------------------------------------------------------------------
    static void defaultBezScheduler() {
        Flux.range(1, 3)
                .map(n -> n * 10)
                .doOnNext(v -> log("default", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Pomoćna metoda - isti tok, ali sa subscribeOn na zadatom scheduler-u.
    // Ispisujemo ime niti za svaki element - to je jedini način da
    // "vidimo" gde se rad zaista izvršava.
    // -------------------------------------------------------------------
    static void with(Scheduler scheduler, String tag) {
        Flux.range(1, 3)
                .map(n -> n * 10)
                .subscribeOn(scheduler)
                .doOnNext(v -> log(tag, v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // parallel ima broj niti = Runtime.availableProcessors().
    // Sa flatMap koji svakom elementu daje DELAY na parallel pool-u,
    // vidimo da se rad razdeli po nitima parallel-1, parallel-2, ...
    //
    // Praktično: ako pokrenemo ovaj test sa 8 jezgara, dobićemo 8 niti
    // parallel-1..parallel-8.
    // -------------------------------------------------------------------
    static void parallelRaspodela() {
        Flux.range(1, 8)
                .flatMap(n -> Flux.just(n)
                        .delayElements(Duration.ofMillis(50))    // delay → nit parallel-X
                        .doOnNext(v -> log("rail", v)))
                .blockLast();

        log("info", "broj jezgara = " + Runtime.getRuntime().availableProcessors());
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
