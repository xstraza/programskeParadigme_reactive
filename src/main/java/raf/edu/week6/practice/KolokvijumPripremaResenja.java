package raf.edu.week6.practice;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 6 - rešenja zadataka iz {@link KolokvijumPriprema}.
 *
 * Svako rešenje je samostalno i pokrenuto iz svoje metode. Tamo gde tok
 * teče asinhrono (delay, scheduler, interval), na kraju se koristi
 * {@code block()/blockLast()} da se main ne završi pre nego što signali
 * stignu - to je jedina svrha blokiranja ovde.
 */
public class KolokvijumPripremaResenja {

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
    // Zadatak 1 - isti operatori kao Stream + lenjost toka   [nedelja 1]
    // ===================================================================
    static void zadatak1() {
        List<Integer> ulaz = List.of(1, 2, 3, 4, 5, 6);

        // (a) isti operatori, isto značenje - Stream (pull) i Flux (push)
        List<Integer> streamRez = ulaz.stream()
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .toList();
        log("stream", streamRez);

        List<Integer> fluxRez = Flux.fromIterable(ulaz)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .collectList()
                .block();
        log("flux", fluxRez);

        // (b) lenjost: pipeline je samo "nacrt" dok ga ne pokrenemo
        Flux<Integer> nacrt = Flux.fromIterable(ulaz)
                .doOnNext(n -> log("radim", n));

        log("info", "tok je sastavljen, ali NIJE pokrenut - iznad nema 'radim'");
        log("info", "sada ga pokrećem (subscribe):");
        nacrt.blockLast();                       // tek subscribe/blockLast pokreće tok
    }

    // ===================================================================
    // Zadatak 2 - cold izvor, transformacija i tajming   [nedelja 2]
    // ===================================================================
    static void zadatak2() {
        Flux.just("paradigme", "mono", "flux", "reaktivno", "tok", "backpressure")
                .filter(w -> w.length() > 4)
                .map(String::toUpperCase)
                .take(3)
                .delayElements(Duration.ofMillis(200))
                .doOnSubscribe(s -> log("lifecycle", "START"))
                .doOnNext(w -> log("rec", w))
                .doOnComplete(() -> log("lifecycle", "KRAJ"))
                .blockLast();                    // drži main dok 200ms emisije ne prođu
    }

    // ===================================================================
    // Zadatak 3 - spajanje tri nezavisna asinhrona izvora   [nedelja 3/4]
    // ===================================================================
    static void zadatak3() {
        Mono<String>  ime   = Mono.delay(Duration.ofMillis(120)).map(t -> "Ana");
        Mono<String>  grad  = Mono.delay(Duration.ofMillis(200)).map(t -> "Beograd");
        Mono<Integer> poeni = Mono.delay(Duration.ofMillis(150)).map(t -> 87);

        long t0 = System.currentTimeMillis();

        String rez = Mono.zip(ime, grad, poeni)
                .map(t -> t.getT1() + " iz grada " + t.getT2() + " ima " + t.getT3() + " poena")
                .block();                        // čeka sve tri grane

        log("rezultat", rez + " (" + (System.currentTimeMillis() - t0) + "ms)");
    }

    // ===================================================================
    // Zadatak 4 - redosled kod paralelne obrade   [nedelja 3]
    // ===================================================================
    static void zadatak4() {
        log("info", "(a) redom kako stignu - flatMap:");
        Flux.range(1, 5)
                .flatMap(n -> Mono.delay(Duration.ofMillis((6 - n) * 100L)).map(t -> n))
                .doOnNext(n -> log("stigao", n))
                .blockLast();

        log("info", "(b) redosled izvora - concatMap:");
        Flux.range(1, 5)
                .concatMap(n -> Mono.delay(Duration.ofMillis((6 - n) * 100L)).map(t -> n))
                .doOnNext(n -> log("redom", n))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 5 - grupisanje   [nedelja 3]
    // ===================================================================
    static void zadatak5() {
        // (a) grupe od po 5 uzastopnih
        Flux.range(1, 20)
                .buffer(5)
                .doOnNext(grupa -> log("grupa", grupa))
                .blockLast();

        // (b) parni vs neparni - prebroj po grupi
        Map<Boolean, Long> brojanje = Flux.range(1, 20)
                .groupBy(n -> n % 2 == 0)
                .flatMap(g -> g.count().map(c -> Map.entry(g.key(), c)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .block();

        log("grupisanje", "parnih=" + brojanje.get(true)
                + ", neparnih=" + brojanje.get(false));
    }

    // ===================================================================
    // Zadatak 6 - "search box", pobeđuje poslednji   [nedelja 3]
    // ===================================================================
    static void zadatak6() {
        Flux<String> upiti = Flux.just("r", "re", "rea", "reac")
                .delayElements(Duration.ofMillis(100));

        upiti.switchMap(q -> Mono.delay(Duration.ofMillis(150))
                        .map(t -> "rezultat: " + q))   // novi upit otkazuje staru pretragu
                .doOnNext(r -> log("prikaz", r))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 7 - razdvajanje blokirajuće i CPU faze po nitima [nedelja 4]
    // ===================================================================
    static void zadatak7() {
        Flux.range(1, 4)
                .flatMap(n -> Mono.fromCallable(() -> {
                                    sleep(100);                    // blokirajuće "učitavanje"
                                    log("ucitavanje", n);
                                    return n;
                                })
                                .subscribeOn(Schedulers.boundedElastic()))  // blocking pool
                .publishOn(Schedulers.parallel())                 // od ovde: CPU pool
                .map(n -> {
                    int sq = n * n;
                    log("kvadrat", n + " -> " + sq);
                    return sq;
                })
                .blockLast();
    }

    // ===================================================================
    // Zadatak 8 - stvarni paralelizam preko više niti   [nedelja 4]
    // ===================================================================
    static void zadatak8() {
        long t0 = System.currentTimeMillis();

        Flux.range(1, 8)
                .parallel(4)                       // 4 paralelne "trake"
                .runOn(Schedulers.parallel())      // svaka na CPU worker-u
                .map(n -> {
                    sleep(200);                    // simulacija CPU posla
                    log("obrada", n);
                    return n * n;
                })
                .sequential()                      // nazad u običan Flux
                .blockLast();

        log("vreme", (System.currentTimeMillis() - t0) + "ms (serijski bi bilo ~1600ms)");
    }

    // ===================================================================
    // Zadatak 9 - brz izvor, spor potrošač + preskakanje grešaka [ned. 5]
    // ===================================================================
    static void zadatak9() {
        Flux.interval(Duration.ofMillis(1))        // brz producer: 0,1,2,...
                .onBackpressureLatest()            // čuvaj samo najnoviju vrednost
                .publishOn(Schedulers.boundedElastic(), 1)
                .concatMap(n -> Mono.fromCallable(() -> {
                            sleep(50);             // spor consumer
                            if (n % 7 == 0) throw new RuntimeException("loš " + n);
                            return n;
                        }).onErrorResume(ex -> Mono.empty()))   // tiho preskoči loš element
                .take(15)                          // ograniči izlaz da se demo završi
                .doOnNext(n -> log("obradjeno", n))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 10 - otporni dashboard + sabirnica i stanje   [nedelja 5/6]
    // ===================================================================
    static void zadatak10() {
        Retry politika = Retry.backoff(2, Duration.ofMillis(50))
                .filter(ex -> ex instanceof IOException
                           || ex instanceof TimeoutException);

        // Sabirnica događaja sa dva posmatrača.
        Sinks.Many<String> bus = Sinks.many().multicast().onBackpressureBuffer();
        bus.asFlux().subscribe(v -> log("posmatrac-1", v));
        bus.asFlux().subscribe(v -> log("posmatrac-2", v));

        AtomicInteger uspeha  = new AtomicInteger();
        AtomicInteger rezervi = new AtomicInteger();

        Mono<String> svcA = Mono.fromCallable(() -> { sleep(50); return "A-OK"; })
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> svcB = Mono.<String>fromCallable(() -> { sleep(20); throw new IOException("B pao"); })
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> svcC = Mono.fromCallable(() -> { sleep(800); return "C-OK"; })
                .subscribeOn(Schedulers.boundedElastic());

        String red = Mono.zip(
                        otporno("A", svcA, politika, bus, uspeha, rezervi),
                        otporno("B", svcB, politika, bus, uspeha, rezervi),
                        otporno("C", svcC, politika, bus, uspeha, rezervi))
                .map(t -> "A=" + t.getT1() + " B=" + t.getT2() + " C=" + t.getT3())
                .block();

        log("dashboard", red);
        log("stanje", "uspeha=" + uspeha.get() + ", rezervi=" + rezervi.get());
    }

    /**
     * Per-servis zaštita: rok 300ms -> retry sa backoff-om (samo prolazne
     * greške) -> rezerva "N/A". Svaki ishod ide na sabirnicu i ažurira
     * tekuće stanje.
     */
    static Mono<String> otporno(String naziv,
                                Mono<String> servis,
                                Retry politika,
                                Sinks.Many<String> bus,
                                AtomicInteger uspeha,
                                AtomicInteger rezervi) {
        return servis
                .timeout(Duration.ofMillis(300))
                .retryWhen(politika)
                .map(v -> { uspeha.incrementAndGet(); return v; })
                .onErrorReturn("N/A")
                .doOnNext(v -> {
                    if (v.equals("N/A")) rezervi.incrementAndGet();
                    // busyLooping handler: bezbedno čak i ako tri grane emituju uporedo
                    bus.emitNext(naziv + "=" + v,
                            Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
                    log("stanje", "uspeha=" + uspeha.get() + ", rezervi=" + rezervi.get());
                });
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-13s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
