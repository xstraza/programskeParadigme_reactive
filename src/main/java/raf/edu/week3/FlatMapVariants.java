package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 3 — flatMap, concatMap, flatMapSequential.
 *
 * Sva tri operatora rade istu STVAR: za svaki element ulaznog toka
 * pozovu funkciju T → Publisher&lt;R&gt; (Mono ili Flux), dobiju "unutrašnji"
 * tok i njegove rezultate "spuste" u izlazni tok.
 *
 * Razlika je u tome KAKO se unutrašnji tokovi vode:
 *
 *   flatMap            — pretplati se na sve unutrašnje tokove ODMAH,
 *                        rezultati se INTERLEAVE-uju (redosled NIJE
 *                        garantovan). Maksimalan paralelizam.
 *
 *   concatMap          — sledeći unutrašnji tok ne počinje dok prethodni
 *                        ne završi. Redosled GARANTOVAN. Bez paralelizma.
 *
 *   flatMapSequential  — pretplati se odmah (kao flatMap), ali bafera
 *                        rezultate i izlaže ih po redu ulaza. Redosled
 *                        garantovan + paralelizam.
 *
 * Ovo je SRCE reaktivne kompozicije — analog je flatMap-u nad
 * Stream-om iz FP dela semestra, samo što unutra može da bude
 * asinhroni rad (HTTP poziv, baza, drugi reaktivni izvor).
 */
public class FlatMapVariants {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. flatMap — async transform, interleaving ===\n");
        flatMapDemo();

        System.out.println("\n=== 2. concatMap — serijski, redosled garantovan ===\n");
        concatMapDemo();

        System.out.println("\n=== 3. flatMapSequential — paralelno + redosled ===\n");
        flatMapSequentialDemo();

        System.out.println("\n=== 4. flatMap sa Mono — najčešći async pattern ===\n");
        flatMapWithMono();

        System.out.println("\n=== 5. flatMap vs map — zašto vraćaš Publisher ===\n");
        flatMapVsMap();

        System.out.println("\n=== 6. concurrency parametar flatMap-a ===\n");
        flatMapConcurrency();
    }

    // -------------------------------------------------------------------
    // flatMap — odmah pretplati sve unutrašnje tokove. Rezultati se
    // INTERLEAVE-uju onako kako stignu kroz vreme.
    //
    // Ovde svaki ulazni element 1..3 mapiramo u Flux koji emituje
    // "elem-x" sa malim delay-om. Vidimo da rezultati izlaze pomešano.
    // -------------------------------------------------------------------
    static void flatMapDemo() {
        Flux.just(1, 2, 3)
                .flatMap(n -> Flux.just(n + "a", n + "b")
                        .delayElements(Duration.ofMillis(100 * n)))   // razl. brzine
                .doOnNext(v -> log("flatMap", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // concatMap — RED PO RED. Sledeći unutrašnji tok počinje tek kada
    // prethodni završi (onComplete). Bez paralelizma.
    //
    // Koristi se kada redosled rezultata MORA da prati redosled ulaza:
    //   - obrada transakcija po vremenu
    //   - upis u bazu gde redosled važi
    //   - paginacija (sledeća stranica tek kad prethodna stigne)
    // -------------------------------------------------------------------
    static void concatMapDemo() {
        Flux.just(1, 2, 3)
                .concatMap(n -> Flux.just(n + "a", n + "b")
                        .delayElements(Duration.ofMillis(100 * n)))
                .doOnNext(v -> log("concatMap", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // flatMapSequential — kompromis: pretplati se ODMAH (paralelizam),
    // ali bafera i izlaže rezultate U REDU PO ULAZU.
    //
    // Misli na njega kao "Promise.all" iz JS-a: pokreni sve, čekaj sve,
    // vrati po redu.
    // -------------------------------------------------------------------
    static void flatMapSequentialDemo() {
        Flux.just(1, 2, 3)
                .flatMapSequential(n -> Flux.just(n + "a", n + "b")
                        .delayElements(Duration.ofMillis(100 * n)))
                .doOnNext(v -> log("flatMapSequential", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // flatMap u praksi — gotovo uvek ide iz T u Mono<R>.
    //
    // Tipičan pattern: imamo Flux<UserId> i za svaki id treba da pozovemo
    // userService.findById(id) koji vraća Mono<User>. flatMap je upravo
    // taj "spojnik" — bez njega bismo imali Flux<Mono<User>> (ugnježdeno!).
    // -------------------------------------------------------------------
    static void flatMapWithMono() {
        Flux<Integer> userIds = Flux.just(101, 102, 103);

        userIds
                .flatMap(FlatMapVariants::findUserById)
                .doOnNext(u -> log("findUserById", u))
                .blockLast();
    }

    // Simulira async REST poziv koji vraća Mono<User>.
    static Mono<String> findUserById(int id) {
        return Mono.fromCallable(() -> "User#" + id)
                .delayElement(Duration.ofMillis(150));
    }

    // -------------------------------------------------------------------
    // flatMap vs map — zašto i kada šta.
    //
    // map        : T → R          (sinhrona transformacija)
    // flatMap    : T → Mono<R>    (asinhrona transformacija)
    //              T → Flux<R>    (1 ulaz → 0..N izlaza)
    //
    // Pokušaj sa map-om gde treba flatMap: dobićeš Flux<Mono<User>>
    // — tok koji emituje OBEĆANJA, ne korisnike. Treba ih "spljoštiti".
    // -------------------------------------------------------------------
    static void flatMapVsMap() {
        // ANTI-PATTERN: map vraća Mono<User>; tip toka postaje Mono<Mono<...>>.
        Mono<Mono<String>> ugnjezdjeno = Mono.just(101)
                .map(FlatMapVariants::findUserById);
        System.out.println("  [anti-pattern] tip: Mono<Mono<...>> = " + ugnjezdjeno);

        // PRAVILNO: flatMap "spljošti" Mono<Mono<User>> u Mono<User>.
        String korisnik = Mono.just(101)
                .flatMap(FlatMapVariants::findUserById)
                .block();
        System.out.println("  [flatMap]      tip: Mono<String> = " + korisnik);
    }

    // -------------------------------------------------------------------
    // flatMap ima drugi parametar — concurrency (max paralelnih unutrašnjih
    // tokova). Default je 256 (Reactor konstanta Queues.SMALL_BUFFER_SIZE).
    //
    // Ako pravimo HTTP klijent koji ne sme da rastrgne udaljeni servis,
    // ograničimo concurrency.
    // -------------------------------------------------------------------
    static void flatMapConcurrency() {
        log("flatMap(concurrency=2)", "samo 2 paralelna unutrasnja toka:");

        Flux.range(1, 5)
                .flatMap(n -> Mono.fromCallable(() -> "rez-" + n)
                                .delayElement(Duration.ofMillis(200))
                                .doOnSubscribe(s -> log("subscribe", "id=" + n)),
                        /* concurrency = */ 2)
                .doOnNext(v -> log("rezultat", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Helper — log sa vremenom i niti.
    // -------------------------------------------------------------------
    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-22s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
