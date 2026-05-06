package raf.edu.week2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Nedelja 2 — Mono&lt;T&gt; vs. Flux&lt;T&gt;.
 *
 * Dva glavna tipa izvora u Project Reactor-u:
 *   - Mono<T>; — 0 ili 1 element + onComplete/onError
 *   - Flux<T>; — 0..N elemenata + onComplete/onError
 *
 * Razdvojeni su NAMERNO (mogli su imati samo Flux). Razlog: tipska
 * tačnost — kompajler i čitač kôda znaju ako će biti najviše jedan
 * element.
 *
 * Demo pokriva:
 *   1. Tipičan Mono — jedan rezultat (npr. HTTP GET).
 *   2. Tipičan Flux — više elemenata (npr. lista iz baze).
 *   3. Mono.empty / Flux.empty — terminalni signal bez vrednosti.
 *   4. Mono.error / Flux.error — terminalni signal sa greškom.
 *   5. Konverzije: Flux -&gt; Mono i Mono -&gt; Flux.
 */
public class MonoVsFluxDemo {

    public static void main(String[] args) {
        System.out.println("=== 1. Mono — 0 ili 1 element ===\n");
        monoBasic();

        System.out.println("\n=== 2. Flux — 0..N elemenata ===\n");
        fluxBasic();

        System.out.println("\n=== 3. Prazni tokovi ===\n");
        emptyDemo();

        System.out.println("\n=== 4. Tokovi koji odmah pukne ===\n");
        errorDemo();

        System.out.println("\n=== 5. Konverzije Mono <-> Flux ===\n");
        conversions();
    }

    // -------------------------------------------------------------------
    // Mono — kao Optional<CompletableFuture<T>>. Tipično:
    //   - jedan REST poziv:        Mono<UserDTO>
    //   - jedan red iz baze:       Mono<Order>
    //   - "uradi i javi gotov":    Mono<Void>
    // -------------------------------------------------------------------
    static void monoBasic() {
        Mono<String> jednaVrednost = Mono.just("Profil korisnika");

        // subscribe sa onNext + onComplete handler-om
        jednaVrednost.subscribe(
                v  -> System.out.println("  [Mono] onNext: " + v),
                err -> System.err.println("  [Mono] onError: " + err),
                () -> System.out.println("  [Mono] onComplete"));
    }

    // -------------------------------------------------------------------
    // Flux — kao Stream koji teče kroz vreme. Tipično:
    //   - lista korisnika:         Flux<User>
    //   - WebSocket poruke:        Flux<Message>
    //   - tikovi tajmera:          Flux<Long>
    // -------------------------------------------------------------------
    static void fluxBasic() {
        Flux<String> viseVrednosti = Flux.just("post1", "post2", "post3");

        viseVrednosti.subscribe(
                v  -> System.out.println("  [Flux] onNext: " + v),
                err -> System.err.println("  [Flux] onError: " + err),
                () -> System.out.println("  [Flux] onComplete"));
    }

    // -------------------------------------------------------------------
    // Prazan tok = SAMO onComplete, bez ijedne vrednosti.
    // U Mono-u to znači "našao sam, ali nema rezultata" (npr. HTTP 404).
    // -------------------------------------------------------------------
    static void emptyDemo() {
        Mono.<String>empty()
                .subscribe(
                        v   -> System.out.println("  [Mono.empty] onNext: " + v),  // NEĆE biti pozvano
                        err -> System.err.println("  [Mono.empty] onError: " + err),
                        ()  -> System.out.println("  [Mono.empty] onComplete (bez vrednosti)"));

        Flux.<Integer>empty()
                .subscribe(
                        v   -> System.out.println("  [Flux.empty] onNext: " + v),
                        err -> System.err.println("  [Flux.empty] onError: " + err),
                        ()  -> System.out.println("  [Flux.empty] onComplete (0 elemenata)"));
    }

    // -------------------------------------------------------------------
    // Greška u reaktivnom svetu nije bačeni izuzetak — to je SIGNAL
    // u toku, ekvivalentan onComplete-u, samo terminalan na drugi način.
    // -------------------------------------------------------------------
    static void errorDemo() {
        Mono.<String>error(new IllegalStateException("nešto je puklo"))
                .subscribe(
                        v   -> System.out.println("  [Mono.error] onNext: " + v),  // NEĆE
                        err -> System.err.println("  [Mono.error] onError: " + err.getMessage()),
                        ()  -> System.out.println("  [Mono.error] onComplete"));     // NEĆE
    }

    // -------------------------------------------------------------------
    // Konverzije.
    // -------------------------------------------------------------------
    static void conversions() {
        // Flux -> Mono.next() — uzmi prvi element, ostatak ignoriši
        Mono<Integer> prvi = Flux.range(10, 5).next();
        System.out.println("  [Flux.next()] prvi: " + prvi.block());

        // Flux -> Mono.collectList() — sakupi sve u List, vrati Mono<List>
        Mono<List<Integer>> svi = Flux.range(10, 5).collectList();
        System.out.println("  [Flux.collectList()] svi: " + svi.block());

        // Mono -> Flux — samo "podigni" tip
        Flux<String> kaoFlux = Mono.just("samo-jedan").flux();
        kaoFlux.subscribe(v -> System.out.println("  [Mono.flux()] onNext: " + v));
    }
}
