package raf.edu.week5;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeoutException;

/**
 * Nedelja 5 - error handling u Reactor-u.
 *
 * Greška u reaktivnom toku NIJE običan throw - prolazi kao terminal
 * signal onError(Throwable). Kad jednom onError prođe, tok je MRTAV -
 * neće biti više elemenata.
 *
 * Da bismo "preživeli" grešku, moramo da PREUSMERIMO tok PRE nego što
 * stigne do subscribe-a. Operatori za to:
 *
 *   onErrorReturn(fallback)   - zameni grešku konkretnom vrednošću
 *   onErrorResume(fn)         - zameni grešku drugim Publisher-om
 *   onErrorContinue(fn)       - preskoči loš element i nastavi
 *   onErrorMap(fn)            - samo zameni TIP exception-a, ne hvataš
 *   doOnError(fn)             - side-effect (log), NE HVATA grešku
 *
 * Demo pokazuje sve varijante na istom toku da bi se videla razlika u
 * ponašanju.
 */
public class ErrorHandlingDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. Bez handling-a - tok pada, ostali elementi se ne emituju ===\n");
        primer1_bezHandlinga();

        System.out.println("\n=== 2. onErrorReturn - default vrednost ===\n");
        primer2_onErrorReturn();

        System.out.println("\n=== 3. onErrorResume - fallback Publisher ===\n");
        primer3_onErrorResume();

        System.out.println("\n=== 4. onErrorContinue - preskoči loš element ===\n");
        primer4_onErrorContinue();

        System.out.println("\n=== 5. Eksplicitno hvatanje u flatMap-u (preporučeni način) ===\n");
        primer5_flatMapCatch();

        System.out.println("\n=== 6. onErrorMap - menja tip exception-a, ne hvata ===\n");
        primer6_onErrorMap();

        System.out.println("\n=== 7. doOnError - log + nastavi handling ===\n");
        primer7_doOnError();

        System.out.println("\n=== 8. Cascading fallback - primary → cache → default ===\n");
        primer8_cascading();

        System.out.println("\n=== 9. onErrorReturn po tipu - selektivno hvatanje ===\n");
        primer9_poTipu();
    }

    // -------------------------------------------------------------------
    // BEZ handling-a: greška na elementu 3 ubija tok. Elementi 4 i 5 se
    // NIKADA ne emituju, subscribe dobija onError(...) i kraj.
    //
    // Ovo je "default" ponašanje - greška je terminal signal.
    // -------------------------------------------------------------------
    static void primer1_bezHandlinga() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err.getClass().getSimpleName() + ": " + err.getMessage()),
                        () -> log("kraj", "onComplete")          // NEĆE se izvršiti
                );
    }

    // -------------------------------------------------------------------
    // onErrorReturn: ako padne, emituje jedan onNext sa fallback-om i
    // odmah kompletira. Ostali elementi iz IZVORA SU IZGUBLJENI - razlog
    // je što je upstream već poslao onError, mi smo ga samo "preveli".
    //
    // Posledica: vidimo 1, 2, "fallback-9999", onComplete. NEMA 4, 5.
    // -------------------------------------------------------------------
    static void primer2_onErrorReturn() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .onErrorReturn(-9999)
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err),                // ne stiže
                        () -> log("kraj", "onComplete")
                );
    }

    // -------------------------------------------------------------------
    // onErrorResume: posle pada PREĐE na novi Publisher. Možemo da
    // emitujemo više elemenata, ili da pozovemo neki drugi async izvor.
    //
    // Vidimo: 1, 2, "fallback-A", "fallback-B", onComplete.
    // -------------------------------------------------------------------
    static void primer3_onErrorResume() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .map(String::valueOf)
                .onErrorResume(ex -> {
                    log("resume", "pao tok, prebacujem na fallback");
                    return Flux.just("fallback-A", "fallback-B");
                })
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err),
                        () -> log("kraj", "onComplete")
                );
    }

    // -------------------------------------------------------------------
    // onErrorContinue: jedinstveni operator - umesto da ubije tok,
    // preskoči POJEDINAČNI loš element i ide dalje.
    //
    // Vidimo: 10, 20, [preskočeno 3], 40, 50, onComplete.
    //
    // Suptilna semantika: ne svi operatori uzvodno podržavaju ovo.
    // Map i flatMap rade, ali kompleksniji operator-i mogu da se
    // ponašaju neočekivano. Reactor tim preporučuje eksplicitno
    // hvatanje u flatMap-u (videti primer 5).
    // -------------------------------------------------------------------
    static void primer4_onErrorContinue() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .map(n -> n * 10)
                .onErrorContinue((err, badItem) ->
                        log("preskačem", "element=" + badItem + " razlog=" + err.getMessage()))
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err),
                        () -> log("kraj", "onComplete")
                );
    }

    // -------------------------------------------------------------------
    // Preporučeni način "skip on error" - hvatamo u flatMap-u, granica
    // greške je VIDLJIVA. Svaki element ima svoj Mono pipeline, i ako
    // taj pipeline pukne, vraćamo Mono.empty() (znači: ovaj element se
    // ne propagira dalje).
    //
    // Daje istu funkcionalnost kao onErrorContinue, ali bez magije.
    // -------------------------------------------------------------------
    static void primer5_flatMapCatch() {
        Flux.range(1, 5)
                .flatMap(n -> Mono.fromCallable(() -> throwZa3(n))
                        .map(x -> x * 10)
                        .onErrorResume(ex -> {
                            log("preskačem", "element=" + n + " razlog=" + ex.getMessage());
                            return Mono.empty();                  // tiho preskoči
                        }))
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err),
                        () -> log("kraj", "onComplete")
                );
    }

    // -------------------------------------------------------------------
    // onErrorMap: NE hvata grešku, samo zameni tip. Tok i dalje propada
    // onError signalom, ali sa drugačijim exception-om.
    //
    // Tipičan slučaj: prevod između slojeva - SQLException iz DAO sloja
    // ne sme da iscuri u service sloj, pa ga mapiramo u domain exception.
    // -------------------------------------------------------------------
    static void primer6_onErrorMap() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .onErrorMap(IllegalStateException.class,
                        ex -> new RuntimeException("DOMENSKA: " + ex.getMessage(), ex))
                .subscribe(
                        v -> log("vrednost", v),
                        err -> log("greška", err.getClass().getSimpleName() + ": " + err.getMessage()),
                        () -> log("kraj", "onComplete")
                );
    }

    // -------------------------------------------------------------------
    // doOnError: side-effect operator. Idealan za log/metriku, ali NE
    // hvata grešku - tok i dalje pada.
    //
    // Tipičan pattern: doOnError za log, pa onErrorResume za pravi
    // handling. Greška ide kroz oba.
    // -------------------------------------------------------------------
    static void primer7_doOnError() {
        Flux.range(1, 5)
                .map(ErrorHandlingDemo::throwZa3)
                .doOnError(ex -> log("log", "AUDIT: " + ex.getMessage()))
                .onErrorResume(ex -> {
                    log("resume", "fallback ide nakon log-a");
                    return Flux.just(-1);
                })
                .subscribe(v -> log("vrednost", v));
    }

    // -------------------------------------------------------------------
    // Cascading fallback: primary → cache → default. Svaka greška
    // pokreće sledeći fallback. Ako i poslednji padne, propagira se.
    //
    // Stvarno korisno u sistemima sa više nivoa redundancije.
    // -------------------------------------------------------------------
    static void primer8_cascading() {
        primary("config")
                .onErrorResume(ex -> {
                    log("fallback-1", "primary pao, idemo cache");
                    return cache("config");
                })
                .onErrorResume(ex -> {
                    log("fallback-2", "cache pao, idemo default");
                    return Mono.just("default-value");
                })
                .subscribe(v -> log("rezultat", v));
    }

    // -------------------------------------------------------------------
    // onErrorReturn po tipu greške - selektivno. Razlikujemo timeout
    // od I/O greške, dajemo drugi default.
    //
    // Sve ostale tipove (NPE, IAE, ...) i dalje propagiraju - to je
    // dobro, jer NPE je bug, ne tranzijentna greška.
    // -------------------------------------------------------------------
    static void primer9_poTipu() {
        Mono<String> mono1 = Mono.<String>error(new TimeoutException("isteklo"))
                .onErrorReturn(TimeoutException.class, "fallback-na-timeout")
                .onErrorReturn(IOException.class, "fallback-na-io");

        Mono<String> mono2 = Mono.<String>error(new IOException("mreža"))
                .onErrorReturn(TimeoutException.class, "fallback-na-timeout")
                .onErrorReturn(IOException.class, "fallback-na-io");

        log("timeout-case", mono1.block());
        log("io-case", mono2.block());
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    /** Pada kad n == 3. */
    static int throwZa3(int n) {
        if (n == 3) throw new IllegalStateException("loš element " + n);
        return n;
    }

    static Mono<String> primary(String key) {
        return Mono.error(new RuntimeException("primary DOWN"));
    }

    static Mono<String> cache(String key) {
        return Mono.error(new RuntimeException("cache MISS"));
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
