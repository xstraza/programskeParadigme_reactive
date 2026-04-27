package raf.edu.week1;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 1 — Stream API vs. Reactive Stream
 *
 * Side-by-side poređenje. ISTI pipeline:
 *   1, 2, 3, 4, 5, 6
 *     ↓ filter (parno)
 *     ↓ map (kvadrat)
 *     ↓ forEach (print)
 *
 * Razlike:
 *   - Stream je sinhron i pull. Sve se desi odmah.
 *   - Flux je asinhron i push. Sa delayElements vidimo elemente kako
 *     "stižu kroz vreme".
 *   - Operatori map / filter imaju identično značenje.
 *
 * Cilj demo-a: studenti vide da se reaktivni operatori ne uče "ispočetka"
 * — to je isto što i Stream, samo sa vremenom kao prvoklasnim pojmom.
 */
public class StreamVsReactive {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== Stream<Integer> — sinhrono, sve odmah ===\n");
        streamVerzija();

        System.out.println("\n=== Flux<Integer> — sinhrono, sve odmah (bez delay-a) ===\n");
        fluxBezDelay();

        System.out.println("\n=== Flux<Integer> — sa delayElements (300ms) — vremenski razvučeno ===\n");
        fluxSaDelay();

        System.out.println("\n=== Glavna pouka — operatori isti, kontekst drugačiji ===\n");
        zakljucak();
    }

    // -----------------------------------------------------------------------
    // Stream — pull, sinhrono. Terminalna operacija (forEach) "vuče"
    // elemente kroz pipeline.
    // -----------------------------------------------------------------------
    static void streamVerzija() {
        long start = System.currentTimeMillis();

        List.of(1, 2, 3, 4, 5, 6).stream()
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .forEach(n -> log("Stream", n));

        System.out.println("  → Stream gotov za " + (System.currentTimeMillis() - start) + " ms");
    }

    // -----------------------------------------------------------------------
    // Flux bez delay-a — sinhrono kad se subscribe-uje na main niti.
    // Vidi se da i bez delay-a, model je push: subscribe pokreće tok,
    // operator-i ne "vuku" elemente.
    // -----------------------------------------------------------------------
    static void fluxBezDelay() {
        long start = System.currentTimeMillis();

        Flux.just(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .subscribe(n -> log("Flux-sync", n));

        System.out.println("  → Flux (bez delay-a) gotov za " + (System.currentTimeMillis() - start) + " ms");
    }

    // -----------------------------------------------------------------------
    // Flux sa delayElements — sad razlika postaje očigledna.
    // Tok stiže kroz vreme. block() na kraju je tu samo da main čeka
    // dok se tok ne završi (u realnoj aplikaciji nikad ne bismo
    // blokirali — videti komentar dole).
    // -----------------------------------------------------------------------
    static void fluxSaDelay() {
        long start = System.currentTimeMillis();

        Flux.just(1, 2, 3, 4, 5, 6)
                .filter(n -> n % 2 == 0)
                .map(n -> n * n)
                .delayElements(Duration.ofMillis(300))
                .doOnNext(n -> log("Flux-async", n))
                .blockLast();   // <-- blokiramo MAIN samo zato što je demo;
                                // u produkciji bi se subscribe vratio i nastavila bi
                                // se "prava" reaktivna obrada.

        System.out.println("  → Flux (sa 300ms delay) gotov za " + (System.currentTimeMillis() - start) + " ms");
    }

    // -----------------------------------------------------------------------
    // Pomoćno logovanje sa vremenom + nit, da se vidi razlika u tome
    // KAD i NA KOJOJ niti elementi stižu.
    // -----------------------------------------------------------------------
    static void log(String tag, Object value) {
        System.out.printf("  [%s] %s on %s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }

    static void zakljucak() {
        System.out.println("""
                  - filter(Predicate<T>) i map(Function<T, R>) imaju ISTO značenje
                    i u Stream-u i u Flux-u. Učimo apstrakciju, ne sintaksu.
                  - Razlika je u tome KAD i KAKO elementi stižu:
                      * Stream — pull, sve odmah, jedna nit.
                      * Flux   — push, kroz vreme, scheduler bira nit.
                  - block()/blockLast() koristimo SAMO u demo/test kodu.
                    U produkciji subscribe vraća kontrolu i tok teče u pozadini.
                """);
    }
}
