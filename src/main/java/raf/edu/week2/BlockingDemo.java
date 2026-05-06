package raf.edu.week2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Nedelja 2 — block(), blockFirst(), blockLast(), blockOptional().
 *
 * Ceo poenta reaktivnog modela je NEBLOKIRAJUĆE izvršavanje.
 * Pa zašto onda u demo kodu skoro stalno vidimo .block() na kraju?
 *
 * Odgovor: zato što `main` nije reaktivni kontekst. subscribe() je
 * non-blocking — vraća se ODMAH, a tok teče u pozadini. Ako main
 * ne čeka, JVM se ugasi.
 *
 * Ovaj demo pokazuje:
 *   1. Šta se desi BEZ block-a (pokaze zašto demo treba block).
 *   2. block() na Mono-u — vraća T (ili null).
 *   3. blockOptional() — sigurnija verzija, vraća Optional<T>.
 *   4. blockFirst / blockLast — Flux varijante.
 *   5. block(Duration) — sa timeout-om.
 *   6. NEGATIVAN PRIMER — block u operatoru. NIKAD!
 */
public class BlockingDemo {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. Bez block — JVM se gasi pre kraja toka ===\n");
        bezBlock();

        System.out.println("\n=== 2. Mono.block() — vrati T ===\n");
        monoBlock();

        System.out.println("\n=== 3. Mono.blockOptional() — sigurnija varijanta ===\n");
        monoBlockOptional();

        System.out.println("\n=== 4. Flux.blockFirst() / blockLast() ===\n");
        fluxBlock();

        System.out.println("\n=== 5. block(Duration) — sa timeout-om ===\n");
        blockSaTimeoutom();

        System.out.println("\n=== 6. KAD NE BLOKIRATI — block u operatoru ===\n");
        kadNeBlokirati();
    }

    // -------------------------------------------------------------------
    // Bez block, async tok ne stigne pre nego što main završi.
    // (Ovo demonstriramo sa kratkim sleep-om da bismo videli da je
    //  tok bar POČEO — bez sleep-a JVM bi se ugasio pre subscribe-a.)
    // -------------------------------------------------------------------
    static void bezBlock() throws InterruptedException {
        Flux.interval(Duration.ofMillis(50))
                .take(10)
                .doOnNext(n -> System.out.println("  [bez-block] " + n))
                .subscribe();   // NE čekamo

        System.out.println("  [main] subscribe vraćen ODMAH; main ide dalje...");
        Thread.sleep(150);      // dam mu malo da nešto stigne, da je vidljivo
        System.out.println("  [main] kraj demo (gomila tikova ne stigne uopšte)");
    }

    // -------------------------------------------------------------------
    // Mono.block() — sinhroni "izvuci vrednost". Vrati T, ili null ako
    // je tok bio prazan, ili throw RuntimeException ako je tok pukao.
    // -------------------------------------------------------------------
    static void monoBlock() {
        String v = Mono.just("dobio sam ja")
                .delayElement(Duration.ofMillis(100))
                .block();

        System.out.println("  [block]      " + v);

        // Mono.empty().block() — vraća null
        String prazan = Mono.<String>empty().block();
        System.out.println("  [block null] " + prazan);
    }

    // -------------------------------------------------------------------
    // blockOptional — bezbedno za prazan tok, vraća Optional<T>.
    // -------------------------------------------------------------------
    static void monoBlockOptional() {
        Optional<String> ima = Mono.just("ima").blockOptional();
        Optional<String> nema = Mono.<String>empty().blockOptional();

        System.out.println("  [blockOptional/ima]   " + ima);
        System.out.println("  [blockOptional/nema]  " + nema);
    }

    // -------------------------------------------------------------------
    // Flux ima dve varijante:
    //   - blockFirst — uzmi prvi pa otkaži ostatak
    //   - blockLast  — čekaj do kraja, vrati poslednji
    // -------------------------------------------------------------------
    static void fluxBlock() {
        Integer prvi = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(50))
                .blockFirst();
        System.out.println("  [blockFirst] " + prvi);

        Integer poslednji = Flux.range(1, 5)
                .delayElements(Duration.ofMillis(50))
                .blockLast();
        System.out.println("  [blockLast]  " + poslednji);

        // Ako želimo SVE elemente, sakupimo ih prvo u List, pa block.
        List<Integer> svi = Flux.range(1, 5)
                .collectList()
                .block();
        System.out.println("  [collect+block] " + svi);
    }

    // -------------------------------------------------------------------
    // block(Duration) — defanzivnost. Ako se tok ne završi za dato
    // vreme, baca IllegalStateException.
    // -------------------------------------------------------------------
    static void blockSaTimeoutom() {
        try {
            Mono.just("kasnim")
                    .delayElement(Duration.ofSeconds(2))
                    .block(Duration.ofMillis(200));   // tok traje 2s, mi čekamo 200ms
        } catch (IllegalStateException e) {
            System.out.println("  [block(200ms)] bacio: " + e.getClass().getSimpleName()
                    + " — " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------
    // NIKAD ne stavljati .block() unutar operatora. Reactor će u nekim
    // slučajevima i baciti exception (BlockHound integration), a u
    // drugim će tiho potrošiti event-loop nit i sve "zalediti".
    //
    // (Ovde je primer KOMENTARISAN da se demo ne sruši; pravilo je
    //  važnije od izvršenja.)
    // -------------------------------------------------------------------
    static void kadNeBlokirati() {
        System.out.println("""
                  PRAVILO: NIKAD .block() unutar operatora.

                  // ANTI-PATTERN — NE radi ovako:
                  flux.map(id -> Mono.just(fetchUser(id)).block());
                  //                                      ^^^^^^^
                  // Reactor će u nekim slučajevima baciti exception;
                  // u Schedulers.parallel() context-u ovo poreknije
                  // celu nit. Pravilan pristup:
                  //
                  // flux.flatMap(id -> fetchUserMono(id))
                  //                    ^^^^^^^^^^^^^^^^^
                  // (flatMap se uči nedelje 3.)

                  KAD je block() OK:
                    - main metoda (kao u svim ovim demo klasama)
                    - testovi koji moraju da provere finalnu vrednost
                    - CLI alati gde sinhrono čekanje JESTE cilj
                """);
    }
}
