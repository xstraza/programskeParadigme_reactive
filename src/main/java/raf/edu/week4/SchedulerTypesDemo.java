package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
 * Demo ima dva dela:
 *
 *   (A) Primer 1-2: kako se zovu niti svakog pool-a — samo da naučimo
 *       da prepoznamo "parallel-3" / "boundedElastic-5" u logu.
 *
 *   (B) Primer 3-6: PONAŠAJNA razlika između pool-ova. Sa istim
 *       flatMap pattern-om, vidimo da:
 *         - parallel pool razdeli rad na N niti (parallel-1..N),
 *         - boundedElastic isto (boundedElastic-1..M),
 *         - single ostane na jednoj niti BEZ OBZIRA na flatMap,
 *         - immediate uopšte ne prebacuje (sve na main).
 */
public class SchedulerTypesDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. Default - bez ičega, sve radi na 'main' ===\n");
        defaultBezScheduler();

        System.out.println("\n=== 2. Imena niti - jedan tok pušten kroz svaki pool ===\n");
        imenaNiti();

        System.out.println("\n=== 3. parallel - flatMap razdeli posao na više parallel-X niti ===\n");
        viseNitiNa(Schedulers.parallel(), "parallel");

        System.out.println("\n=== 4. boundedElastic - isti pattern za blocking I/O ===\n");
        viseNitiNa(Schedulers.boundedElastic(), "elastic");

        System.out.println("\n=== 5. single - i sa flatMap-om SVE ostaje na single-1 ===\n");
        viseNitiNa(Schedulers.single(), "single");

        System.out.println("\n=== 6. immediate - opt-out, čak ni flatMap ne prebacuje ===\n");
        viseNitiNa(Schedulers.immediate(), "immediate");

        log("info", "broj jezgara (parallel pool size) = " + Runtime.getRuntime().availableProcessors());
    }

    // -------------------------------------------------------------------
    // (A) Default - Publisher ne pravi niti sam. Subscribe na main-u
    //     znači da ceo lanac radi na main-u.
    //
    // Pravilo: tek operator koji ima vremensku dimenziju (delayElements,
    // interval) ili eksplicitan subscribeOn/publishOn prebacuje nit.
    // -------------------------------------------------------------------
    static void defaultBezScheduler() {
        Flux.range(1, 3)
                .map(n -> n * 10)
                .doOnNext(v -> log("main", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // (A) Samo da vidimo kako se zovu niti svakog pool-a. Sve četiri
    //     varijante daju JEDNU nit jer je tok sekvencijalan - tok
    //     pretplate uzme jednog worker-a iz pool-a i radi tu.
    //
    // Primeri 3-6 ispod pokazuju gde se vidi razlika u PONAŠANJU.
    // -------------------------------------------------------------------
    static void imenaNiti() {
        nazivPoola(Schedulers.parallel(), "parallel");
        nazivPoola(Schedulers.boundedElastic(), "elastic");
        nazivPoola(Schedulers.single(), "single");
        nazivPoola(Schedulers.immediate(), "immediate");
    }

    static void nazivPoola(Scheduler scheduler, String tag) {
        Flux.range(1, 2)
                .subscribeOn(scheduler)
                .doOnNext(v -> log(tag, v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // (B) Sad pravo testiranje pool-a. Pokrenemo 5 nezavisnih Mono-a
    //     kroz flatMap. Svaki ima svoj subscribeOn na ISTOM pool-u.
    //
    // Šta očekujemo:
    //   parallel       -> 5 različitih niti parallel-1..parallel-5
    //                     (pool ima dovoljno niti = broj jezgara)
    //   boundedElastic -> 5 različitih niti boundedElastic-1..-5
    //   single         -> SVE 5 na single-1 (po definiciji, samo 1 nit)
    //   immediate      -> SVE 5 na main, jer immediate ne prebacuje
    //
    // Pool je više od imena. Single garantuje serijski rad, immediate
    // ne uvodi nikakvu nit, ostala dva paralelizuju.
    //
    // VAŽNO: logujemo UNUTAR Callable-a (gde runnuje), a ne
    // posle flatMap-a. Razlog: flatMap merger spaja rezultate i
    // doOnNext bi pokazao nit merger-a, ne worker-a koji je radio
    // posao. Da bi se videle prave radne niti, log mora biti u Callable.
    //
    // Thread.sleep stoji da bi se 5 Mono-a stvarno preklopilo vremenski - bez
    // sleep-a prvi bi mogao da završi pre nego drugi krene.
    // -------------------------------------------------------------------
    static void viseNitiNa(Scheduler scheduler, String tag) {
        Flux.range(1, 5)
                .flatMap(n -> Mono.fromCallable(() -> {
                                    log(tag, "obrada " + n);            // log GDE se rad izvršava
                                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                                    return n;
                                })
                                .subscribeOn(scheduler))
                .blockLast();
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
