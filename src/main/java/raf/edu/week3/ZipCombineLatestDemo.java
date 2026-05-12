package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 3 — zip, combineLatest, withLatestFrom.
 *
 * Sve tri kombinuju vise tokova u jedan rezultat, ali NA RAZLICITE
 * NACINE — i ovo su upravo operatori "kojih nema u Stream API-ju" jer
 * podrazumevaju vremensku semantiku.
 *
 *   zip               — saceka po JEDAN element iz SVAKOG izvora,
 *                       spoji ih u tuple, emituje. Brzi izvor CEKA spori.
 *                       Kad neki izvor zavrsi, zip zavrsava.
 *
 *   combineLatest     — uvek emituje na svaki novi element BILO KOG
 *                       izvora, koristeci NAJNOVIJE poznate vrednosti
 *                       iz ostalih. Pocinje tek kada SVAKI izvor jednom
 *                       emituje.
 *
 *   withLatestFrom    — instanca verzija "drugog izvora": glavni tok
 *                       emituje normalno, drugi tok pratimo samo da
 *                       znamo njegovu poslednju vrednost. NE emitujemo
 *                       kad se drugi promeni.
 *
 * Kad sta:
 *   - zip          : "treba mi rezultat SVIH nezavisnih async poziva
 *                     pre nego sto nastavim" (Promise.all u JS-u).
 *   - combineLatest: "UI state koji zavisi od vise nezavisnih izvora,
 *                     i mora da se osvezi cim se bilo koji promeni"
 *                     (npr. filter + sort + paginacija → tabela).
 *   - withLatestFrom: "okini akciju samo kad korisnik klikne, ali mi
 *                      treba i trenutna vrednost iz forme".
 */
public class ZipCombineLatestDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. zip — par po par, ceka sporiji ===\n");
        zipBasic();

        System.out.println("\n=== 2. zip — Promise.all pattern (paralelne async pozive) ===\n");
        zipPromiseAll();

        System.out.println("\n=== 3. zip — razlicit broj elemenata ===\n");
        zipDifferentLengths();

        System.out.println("\n=== 4. combineLatest — uvek najnovije iz svakog ===\n");
        combineLatestDemo();

        System.out.println("\n=== 5. withLatestFrom — glavni tok + uzgredna vrednost ===\n");
        withLatestFromDemo();

        System.out.println("\n=== 6. zip vs combineLatest — jedan primer, dve semantike ===\n");
        zipVsCombineLatest();
    }

    // -------------------------------------------------------------------
    // zip — parovi (1,A), (2,B), (3,C). Brzi izvor CEKA spori.
    //
    // Bitno: zip emituje TEK KADA imaju SVI izvori bar jedan elem.
    // Sledeca emisija — tek kad SVI imaju jos jedan, itd.
    // -------------------------------------------------------------------
    static void zipBasic() {
        Flux<Integer> brojevi = Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(100));
        Flux<String> slova = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(300));   // dva puta sporiji

        Flux.zip(brojevi, slova)
                .doOnNext(t -> log("zip", t.getT1() + "+" + t.getT2()))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Promise.all pattern — pokreni N nezavisnih async poziva paralelno,
    // saceka SVE, zatim formiraj kompozitni rezultat.
    //
    // Bez zip-a: morali bismo da pravimo lance flatMap-ova ili
    // skupljamo CompletableFuture-e — gubimo deklarativnost.
    // -------------------------------------------------------------------
    static void zipPromiseAll() {
        Mono<String> userProfile = simAsync("user-profile", 200);
        Mono<String> userPosts   = simAsync("user-posts",   400);
        Mono<String> userFriends = simAsync("user-friends", 300);

        Mono.zip(userProfile, userPosts, userFriends)
                .doOnNext(t -> log("zip-mono", t.getT1() + " | " + t.getT2() + " | " + t.getT3()))
                .block();
        // Ukupno trajanje: ~400ms (max), ne 900ms (sum).
    }

    static Mono<String> simAsync(String tag, long ms) {
        return Mono.just(tag + "-result")
                .delayElement(Duration.ofMillis(ms))
                .doOnSubscribe(s -> log("start", tag));
    }

    // -------------------------------------------------------------------
    // Kada izvori imaju RAZLICITE duzine, zip zavrsava kad PRVI od njih
    // zavrsi. Ostali "viskovi" se gube.
    // -------------------------------------------------------------------
    static void zipDifferentLengths() {
        Flux<Integer> kratak = Flux.just(1, 2);            // 2 elementa
        Flux<String> dugacak = Flux.just("A", "B", "C", "D"); // 4 elementa

        Flux.zip(kratak, dugacak)
                .doOnNext(t -> log("zip", t.getT1() + "+" + t.getT2()))
                .blockLast();
        // Vidimo: (1,A) (2,B). "C" i "D" se nikad ne emituju.
    }

    // -------------------------------------------------------------------
    // combineLatest — emituje cim se BILO KOJI izvor promeni, koristeci
    // NAJNOVIJE vrednosti svih ostalih.
    //
    // Tipican UI primer: pretraga = (text + filter + sort).
    // Kad korisnik kucne novo slovo, lista se osvezava sa istim filterom
    // i sortom. Kad promeni filter, lista se osvezava sa istim tekstom.
    // -------------------------------------------------------------------
    static void combineLatestDemo() {
        Flux<String> tekst = Flux.just("a", "ab", "abc")
                .delayElements(Duration.ofMillis(150));
        Flux<String> filter = Flux.just("ALL", "ACTIVE")
                .delayElements(Duration.ofMillis(250));

        Flux.combineLatest(tekst, filter, (t, f) -> "tekst=" + t + ", filter=" + f)
                .doOnNext(v -> log("combineLatest", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // withLatestFrom — kao combineLatest, ali emisija se okida SAMO
    // kada se promeni GLAVNI tok. Drugi tok (state) samo "leti pored".
    //
    // Mini-pattern: tok klikova na dugme + tok trenutnih vrednosti
    // forme. Submit se desava na klik, vrednost forme nam je samo
    // potrebna u tom trenutku.
    // -------------------------------------------------------------------
    static void withLatestFromDemo() {
        Flux<String> klikovi = Flux.just("klik1", "klik2", "klik3")
                .delayElements(Duration.ofMillis(300));
        Flux<String> formaState = Flux.just("forma=v1", "forma=v2", "forma=v3", "forma=v4")
                .delayElements(Duration.ofMillis(100));

        klikovi.withLatestFrom(formaState, (k, s) -> k + " sa " + s)
                .doOnNext(v -> log("withLatestFrom", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Direktno poredjenje — isti ulazi, zip vs combineLatest.
    //
    // tokA: 1 (na 100ms), 2 (na 200ms), 3 (na 300ms)
    // tokB: X (na 250ms)
    //
    // zip            : ceka par. Emituje (1, X) cim X stigne. Sledece bi
    //                  bilo (2, ?) — ali B vise nema sta, zavrsi.
    // combineLatest  : ceka da OBA imaju bar jedan. Cim X stigne, emituje
    //                  (2, X) (jer je 2 stigao u 200ms, 1 vec zamenjen).
    //                  Onda na 3: (3, X).
    // -------------------------------------------------------------------
    static void zipVsCombineLatest() {
        Flux<Integer> tokA = Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(100));
        Flux<String> tokB = Mono.just("X")
                .delayElement(Duration.ofMillis(250))
                .flux();

        System.out.println("  [zip]");
        Flux.zip(tokA, tokB)
                .doOnNext(t -> log("zip", t.getT1() + "+" + t.getT2()))
                .blockLast();

        System.out.println("\n  [combineLatest]");
        Flux<Integer> tokA2 = Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(100));
        Flux<String> tokB2 = Mono.just("X")
                .delayElement(Duration.ofMillis(250))
                .flux();

        Flux.combineLatest(tokA2, tokB2, (a, b) -> a + "+" + b)
                .doOnNext(v -> log("combineLatest", v))
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
