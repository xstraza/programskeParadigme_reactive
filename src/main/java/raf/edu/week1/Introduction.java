package raf.edu.week1;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Nedelja 1 — "Hello reactive world" sa Project Reactor-om.
 *
 * Tri ideje za dalje:
 *   1. Mono<T>  — tok od 0 ili 1 elementa  (npr. rezultat HTTP poziva).
 *   2. Flux<T>  — tok od 0..N elemenata    (npr. niz redova iz baze).
 *   3. Operatori se LANČAJU (map, filter, doOnNext, ...) baš kao na
 *      Stream-u — vidi {@link StreamVsReactive}.
 */
public class Introduction {

    public static void main(String[] args) {
        System.out.println("=== Mono<T> — najviše jedan element ===\n");
        monoDemo();

        System.out.println("\n=== Flux<T> — više elemenata, sinhrono ===\n");
        fluxSinhrono();

        System.out.println("\n=== Flux<T> — sa interval-om, asinhrono ===\n");
        fluxAsinhrono();
    }

    // -----------------------------------------------------------------------
    // Mono — 0 ili 1 element. Tipična upotreba: jedan API poziv,
    // jedan red iz baze, jedan rezultat operacije.
    // -----------------------------------------------------------------------
    static void monoDemo() {
        Mono<String> pozdrav = Mono.just("Zdravo, reaktivni svete!")
                .map(String::toUpperCase)
                .doOnNext(s -> System.out.println("  [Mono] vrednost: " + s));

        // block() je OK u demo / main-u — čeka i vraća vrednost.
        // U produkciji se nikad ne poziva block (videti week 5 i week 6).
        String rezultat = pozdrav.block();
        System.out.println("  [Mono] dobijeno preko block(): " + rezultat);

        // Mono može biti i prazan — bez vrednosti, sa onComplete.
        Mono.empty()
                .doOnSuccess(v -> System.out.println("  [Mono.empty] onSuccess sa vrednošću: " + v))
                .block();
    }

    // -----------------------------------------------------------------------
    // Flux — 0..N elemenata. Bez delay-a, ovo je sinhrono kao Stream.
    // -----------------------------------------------------------------------
    static void fluxSinhrono() {
        Flux<Integer> kvadrati = Flux.range(1, 5)        // 1, 2, 3, 4, 5
                .map(n -> n * n);                         // 1, 4, 9, 16, 25

        // subscribe sa Consumer-om — najjednostavniji način da vidimo elemente.
        kvadrati.subscribe(n -> System.out.println("  [Flux-sync] kvadrat: " + n));
    }

    // -----------------------------------------------------------------------
    // Flux sa interval-om — emisija svakih 200ms.
    // Ovde se vidi PRAVA snaga Flux-a: tok kroz vreme.
    // -----------------------------------------------------------------------
    static void fluxAsinhrono() {
        Flux.interval(Duration.ofMillis(200))
                .take(5)                                  // uzmi prvih 5 (inače beskonačan tok!)
                .map(tick -> "Tik #" + tick)
                .doOnNext(s -> System.out.println("  [Flux-async] " + s + " on " + Thread.currentThread().getName()))
                .blockLast();                             // čekamo da svi stignu pre kraja main-a

        System.out.println("""

                Sledeća nedelja:
                  - Mono / Flux u detalje — kreiranje izvora, lifecycle, operatori.
                  - Razlika između cold i hot stream-a.
                  - subscribe() varijante: onNext, onError, onComplete callback-ovi.
                """);
    }
}
