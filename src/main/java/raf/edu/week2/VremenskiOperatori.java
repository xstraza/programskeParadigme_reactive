package raf.edu.week2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 2 — operatori kojih NEMA u Stream-u.
 *
 * Stream API ne zna ništa o vremenu — sve se "desi sad". Reactor-u
 * je vreme prvoklasni pojam. Ovde su operatori koji eksplicitno
 * koriste vreme i operatori koji rade sa "praznim tokom".
 *
 * Pokriva:
 *   1. delayElements / delaySubscription
 *   2. timeout
 *   3. take(Duration) / skip(Duration)
 *   4. defaultIfEmpty / switchIfEmpty
 *   5. repeat
 */
public class VremenskiOperatori {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. delayElements — razmak između emisija ===\n");
        delayElementsDemo();

        System.out.println("\n=== 2. delaySubscription — kasni početak ===\n");
        delaySubscriptionDemo();

        System.out.println("\n=== 3. timeout — padaj ako predugo traje ===\n");
        timeoutDemo();

        System.out.println("\n=== 4. take/skip(Duration) — vremenski prozor ===\n");
        takeSkipDuration();

        System.out.println("\n=== 5. defaultIfEmpty / switchIfEmpty ===\n");
        emptyFallback();

        System.out.println("\n=== 6. repeat — ponovi tok ===\n");
        repeatDemo();
    }

    // -------------------------------------------------------------------
    // delayElements — razmak između sukcesivnih emisija.
    // Vremenski razvuče "Stream-like" izvor da liči na realan event tok.
    // -------------------------------------------------------------------
    static void delayElementsDemo() {
        Flux.range(1, 4)
                .delayElements(Duration.ofMillis(200))
                .doOnNext(n -> log("delayElements", n))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // delaySubscription — odlaže ceo subscribe (kao da kasnimo da
    // pokrenemo).
    // -------------------------------------------------------------------
    static void delaySubscriptionDemo() {
        log("pre-subscribe", "kreiramo Mono");

        Mono.just("kasnio sam pola sekunde")
                .delaySubscription(Duration.ofMillis(500))
                .doOnNext(s -> log("delaySubscription", s))
                .block();
    }

    // -------------------------------------------------------------------
    // timeout — ako se sledeća emisija ne desi za dato vreme, padne
    // sa TimeoutException.
    //
    // Tipičan slučaj: HTTP poziv koji visi.
    // -------------------------------------------------------------------
    static void timeoutDemo() {
        // (a) Brz tok — uspe.
        Mono.just("ok")
                .delayElement(Duration.ofMillis(100))
                .timeout(Duration.ofMillis(500))
                .subscribe(
                        v   -> log("timeout-ok", v),
                        err -> log("timeout-ok", "ERROR: " + err));

        // (b) Spor tok — pukne.
        try {
            Mono.just("kasnim")
                    .delayElement(Duration.ofMillis(500))
                    .timeout(Duration.ofMillis(100))
                    .doOnError(err -> log("timeout-fail", "onError: " + err.getClass().getSimpleName()))
                    .block();
        } catch (Exception e) {
            log("timeout-fail", "block bacio: " + e.getClass().getSimpleName());
        }
    }

    // -------------------------------------------------------------------
    // take(Duration) — uzmi sve što stigne za dato vreme, pa onComplete.
    // skip(Duration) — ignorisi prvih T, pa propusti ostalo.
    // -------------------------------------------------------------------
    static void takeSkipDuration() {
        log("take(Duration)", "uzimam sve sto stigne za 350ms iz interval-a 100ms:");
        Flux.interval(Duration.ofMillis(100))
                .take(Duration.ofMillis(350))
                .doOnNext(n -> log("take(Duration)", n))
                .blockLast();

        log("skip(Duration)", "ignorišem prvih 250ms, pa uzmem 3:");
        Flux.interval(Duration.ofMillis(100))
                .skip(Duration.ofMillis(250))
                .take(3)
                .doOnNext(n -> log("skip(Duration)", n))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // defaultIfEmpty / switchIfEmpty — kad tok završi prazan.
    //
    // defaultIfEmpty(T)         — emituje T ako je tok bio prazan.
    // switchIfEmpty(Mono/Flux)  — prebaci se na drugi izvor ako prazan.
    //
    // switchIfEmpty je idealan za FALLBACK CHAIN (kes -> baza -> API).
    // -------------------------------------------------------------------
    static void emptyFallback() {
        Mono.<String>empty()
                .defaultIfEmpty("default-vrednost")
                .subscribe(v -> System.out.println("  [defaultIfEmpty]  " + v));

        // Fallback chain — kad prvi izvor prazan, pokušaj drugi, pa treći.
        nadjiUKesu()
                .switchIfEmpty(nadjiUBazi())
                .switchIfEmpty(nadjiPrekoApija())
                .subscribe(v -> System.out.println("  [switchIfEmpty]   " + v));
    }

    static Mono<String> nadjiUKesu() {
        System.out.println("  [chain] proverim kes — prazan");
        return Mono.empty();
    }

    static Mono<String> nadjiUBazi() {
        System.out.println("  [chain] proverim bazu — prazan");
        return Mono.empty();
    }

    static Mono<String> nadjiPrekoApija() {
        System.out.println("  [chain] zovnem API — našao!");
        return Mono.just("vrednost-iz-API-ja");
    }

    // -------------------------------------------------------------------
    // repeat — ponovi tok N puta. Okida se na onComplete (NE na error).
    // -------------------------------------------------------------------
    static void repeatDemo() {
        Flux.just("ping")
                .repeat(3)                                // ukupno 4 emisije: original + 3 ponavljanja
                .doOnNext(s -> System.out.println("  [repeat] " + s))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Pomoćno logovanje sa vremenom + nit, da bude vidljivo KAD i NA KOJOJ
    // niti se signal desi.
    // -------------------------------------------------------------------
    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-20s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
