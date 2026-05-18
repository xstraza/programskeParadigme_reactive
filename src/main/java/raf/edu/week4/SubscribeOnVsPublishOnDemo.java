package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 4 - subscribeOn vs publishOn.
 *
 * Ovo su DVA najvažnija operatora za kontrolu niti u Reactor-u, i dva
 * najčešće mešana. Razlika u jednoj rečenici:
 *
 *   subscribeOn  - biramo nit na kojoj se desi SUBSCRIPTION (upstream).
 *                  Mesto u lancu NE utiče. Samo NAJBLIŽI izvoru ima efekta.
 *
 *   publishOn    - biramo nit za sve OPERATORE NIŽE od te tačke.
 *                  Mesto u lancu UTIČE. Više publishOn-ova se nadovezuje.
 *
 * Ovaj demo to pokazuje korak po korak, ispisujući imena niti.
 */
public class SubscribeOnVsPublishOnDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. subscribeOn - sve se dešava na elastic niti ===\n");
        primer1_subscribeOn();

        System.out.println("\n=== 2. publishOn - gornji deo na main, donji na parallel ===\n");
        primer2_publishOn();

        System.out.println("\n=== 3. Više publishOn-ova - lanac menja niti ===\n");
        primer3_visePublishOn();

        System.out.println("\n=== 4. Mesto subscribeOn-a u lancu NIJE bitno ===\n");
        primer4_mestoSubscribeOn();

        System.out.println("\n=== 5. Više subscribeOn-ova - samo prvi (najbliži izvoru) važi ===\n");
        primer5_viseSubscribeOn();

        System.out.println("\n=== 6. Kombinovani - subscribeOn + publishOn ===\n");
        primer6_kombinovani();
    }

    // -------------------------------------------------------------------
    // subscribeOn pomera SUBSCRIPTION na drugi scheduler. To znači da i
    // izvor (range) i svi operatori u lancu izvršavaju se na toj niti
    // - sve dok eksplicitan publishOn ne kaže drugačije.
    // -------------------------------------------------------------------
    static void primer1_subscribeOn() {
        Flux.range(1, 3)
                .doOnNext(n -> log("source", n))            // boundedElastic-X
                .map(n -> n * 10)
                .doOnNext(n -> log("map", n))               // boundedElastic-X
                .subscribeOn(Schedulers.boundedElastic())
                .doOnNext(n -> log("subscribe", n))          // boundedElastic-X
                .blockLast();
    }

    // -------------------------------------------------------------------
    // publishOn ne dira upstream - sve PRE njega ostaje na main niti.
    // Sve POSLE prelazi na parallel.
    // -------------------------------------------------------------------
    static void primer2_publishOn() {
        Flux.range(1, 3)
                .doOnNext(n -> log("pre-publishOn", n))      // main
                .publishOn(Schedulers.parallel())
                .doOnNext(n -> log("post-publishOn", n))     // parallel-X
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Više publishOn-ova - moćan pattern. Svaki segment lanca radi na
    // svom scheduler-u.
    //
    // U produkciji: blocking deo (parsing fajla, JDBC) → boundedElastic,
    // CPU deo (računanje, transformacija) → parallel.
    // -------------------------------------------------------------------
    static void primer3_visePublishOn() {
        Flux.range(1, 3)
                .doOnNext(n -> log("segment-A", n))          // main
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(n -> log("segment-B", n))          // boundedElastic-X
                .publishOn(Schedulers.parallel())
                .doOnNext(n -> log("segment-C", n))          // parallel-X
                .blockLast();
    }

    // -------------------------------------------------------------------
    // subscribeOn može biti BILO GDE u lancu - efekat je isti.
    // Reactor radi subscribe od dna ka vrhu, i kada vidi subscribeOn,
    // ceo upstream se prebacuje.
    // -------------------------------------------------------------------
    static void primer4_mestoSubscribeOn() {
        log("info", "subscribeOn NA POČETKU lanca:");
        Flux.range(1, 2)
                .subscribeOn(Schedulers.boundedElastic())   // NA POČETKU
                .doOnNext(n -> log("pocetak", n))
                .map(n -> n * 10)
                .blockLast();

        log("info", "subscribeOn NA KRAJU lanca:");
        Flux.range(1, 2)
                .doOnNext(n -> log("kraj", n))
                .map(n -> n * 10)
                .subscribeOn(Schedulers.boundedElastic())   // NA KRAJU - isti efekat
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Više subscribeOn-ova - samo NAJBLIŽI izvoru ima efekta. Drugi
    // se ignoriše. (Logički: subscribe putuje od dna ka vrhu i prvi
    // subscribeOn koji vidi postavlja nit; drugi ne radi ništa.)
    // -------------------------------------------------------------------
    static void primer5_viseSubscribeOn() {
        Flux.range(1, 3)
                .doOnNext(n -> log("source", n))
                .subscribeOn(Schedulers.boundedElastic())   // OVAJ pobeđuje
                .map(n -> n * 10)
                .subscribeOn(Schedulers.parallel())          // ignorisan
                .doOnNext(n -> log("end", n))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Tipičan produkcijski lanac:
    //   1) Blocking izvor (Mono.fromCallable u realnosti) → subscribeOn(io)
    //   2) CPU transformacija → publishOn(cpu)
    //   3) Finalni subscribe(...)
    // -------------------------------------------------------------------
    static void primer6_kombinovani() {
        Flux.range(1, 3)
                .doOnNext(n -> log("io-izvor", n))                 // boundedElastic
                .map(n -> {
                    log("io-decode", n);                            // boundedElastic
                    return n;
                })
                .subscribeOn(Schedulers.boundedElastic())          // za sve gore
                .publishOn(Schedulers.parallel())
                .map(n -> {
                    log("cpu-compute", n);                          // parallel-X
                    return n * 100;
                })
                .doOnNext(n -> log("final", n))
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
