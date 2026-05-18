package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 3 - switchMap.
 *
 * switchMap je rođak flatMap-a sa jednom KLJUČNOM razlikom:
 *
 *   - flatMap   : svi unutrašnji tokovi se izvršavaju paralelno,
 *                 rezultati svih dolaze u izlaz.
 *
 *   - switchMap : kada stigne NOVI ulazni element, AKTIVNI unutrašnji
 *                 tok se OTKAZUJE i pokreće se novi. Samo rezultati
 *                 NAJNOVIJEG unutrašnjeg toka stižu u izlaz.
 *
 * Klasičan primer: search-as-you-type / autocomplete.
 * Korisnik kuca "Bgrd" → kucne "a" pa "n" pa "j" pa "a".
 * Za svaki novi karakter pravimo HTTP poziv ka backend-u koji vraća
 * sugestije. Stari pozivi su irelevantni - korisnik je promenio upit.
 * switchMap otkaže prethodni poziv i pokrene novi. Bez switchMap-a,
 * sugestije bi se "pomešale" i pokazale rezultate za stara slova.
 *
 * Drugi tipičan slučaj - UI selekcija:
 *   "izabrao si user-a X" → pokreni Mono&lt;UserDetails&gt;
 *   "izabrao si user-a Y" → otkaži stari fetch, pokreni novi
 */
public class SwitchMapDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. flatMap vs switchMap - ista funkcija, različito ponašanje ===\n");
        flatMapVsSwitchMap();

        System.out.println("\n=== 2. switchMap - autocomplete (search-as-you-type) ===\n");
        autocompleteDemo();

        System.out.println("\n=== 3. switchMap - selekcija u UI, otkazivanje prethodnog fetch-a ===\n");
        userSelectionDemo();

        System.out.println("\n=== 4. doOnCancel - kako vidimo da je prethodni tok otkazan ===\n");
        cancelSignalDemo();
    }

    // -------------------------------------------------------------------
    // Direktno poredjenje. Ista funkcija, isti ulaz - drugacije ponasanje.
    //
    // Ulaz: 1 (brz unutrasnji) → 100ms → 2 (spor) → 100ms → 3 (brz).
    // Unutrasnji tok za N traje 300ms i emituje "rez-N".
    //
    // flatMap   : sva tri unutrasnja toka rade, sva tri rezultata stizu.
    // switchMap : kad stigne 2, unutrasnji tok za 1 se PREKIDA.
    //              kad stigne 3, unutrasnji za 2 se PREKIDA.
    //              Samo "rez-3" stiže.
    // -------------------------------------------------------------------
    static void flatMapVsSwitchMap() {
        Flux<Integer> ulaz = Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(100));

        System.out.println("  [flatMap]");
        ulaz
                .flatMap(SwitchMapDemo::sporPosao)
                .doOnNext(v -> log("flatMap", v))
                .blockLast();

        System.out.println("\n  [switchMap]");
        Flux.just(1, 2, 3)
                .delayElements(Duration.ofMillis(100))
                .switchMap(SwitchMapDemo::sporPosao)
                .doOnNext(v -> log("switchMap", v))
                .blockLast();
    }

    static Mono<String> sporPosao(int n) {
        return Mono.just("rez-" + n)
                .delayElement(Duration.ofMillis(300));
    }

    // -------------------------------------------------------------------
    // Autocomplete - pravi production pattern.
    //
    // Tok ulaza simulira sta korisnik kuca: "B", "Be", "Beo", "Beog", "Beogr".
    // Svaki upit pokrece HTTP poziv (simuliran sa delay-em). Poslednji
    // upit je jedini koji nas zanima - switchMap otkazuje sve prethodne.
    // -------------------------------------------------------------------
    static void autocompleteDemo() {
        Flux<String> kucanje = Flux.just("B", "Be", "Beo", "Beog", "Beogr")
                .delayElements(Duration.ofMillis(80));   // korisnik kuca brzo

        kucanje
                .doOnNext(q -> log("kucanje", "upit = " + q))
                .switchMap(SwitchMapDemo::pretraziSugestije)
                .doOnNext(v -> log("sugestija", v))
                .blockLast();
    }

    // Pretraga sugestija traje 250ms - duze nego razmak izmedju karaktera (80ms).
    // To znaci da pre nego sto stari upit zavrsi, doleti novi, pa switchMap
    // otkazuje stari. Samo poslednji upit ("Beogr") ce zavrsiti.
    static Mono<String> pretraziSugestije(String upit) {
        return Mono.just("sugestije za '" + upit + "'")
                .delayElement(Duration.ofMillis(250))
                .doOnSubscribe(s -> log("api.start", upit))
                .doOnCancel(()  -> log("api.cancel", upit));
    }

    // -------------------------------------------------------------------
    // UI selekcija - korisnik klikne tri usera u nizu. Za svaki bi se
    // pokrenuo fetch detalja; ali samo poslednji izbor je zanimljiv.
    // -------------------------------------------------------------------
    static void userSelectionDemo() {
        Flux<Integer> selekcija = Flux.just(101, 102, 103)
                .delayElements(Duration.ofMillis(150));

        selekcija
                .switchMap(id -> ucitajDetaljeKorisnika(id))
                .doOnNext(d -> log("detalji", d))
                .blockLast();
    }

    static Mono<String> ucitajDetaljeKorisnika(int id) {
        return Mono.just("UserDetails#" + id)
                .delayElement(Duration.ofMillis(400))
                .doOnSubscribe(s -> log("fetch.start", "id=" + id))
                .doOnCancel(()  -> log("fetch.cancel", "id=" + id));
    }

    // -------------------------------------------------------------------
    // Kada switchMap otkaze unutrasnji tok, on salje CANCEL signal.
    // Bilo kakav resource cleanup (zatvaranje konekcije, otkazivanje
    // upita ka bazi) treba uraditi u doOnCancel ili doFinally.
    //
    // Ovo nije teorija - production HTTP klijenti (Reactor Netty, ...)
    // koriste cancel signal da prekinu i mreznu konekciju.
    // -------------------------------------------------------------------
    static void cancelSignalDemo() {
        Flux.just("A", "B")
                .delayElements(Duration.ofMillis(100))
                .switchMap(s -> Mono.just("rezultat-" + s)
                        .delayElement(Duration.ofMillis(300))
                        .doOnSubscribe(sub -> log("start", s))
                        .doFinally(sig -> log("finally", s + " " + sig)))
                .doOnNext(v -> log("rezultat", v))
                .blockLast();
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-14s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
