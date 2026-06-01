package raf.edu.week6.typeahead;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 6 - "search-as-you-type" (typeahead).
 * <p>
 * Klasičan reaktivni problem: korisnik kuca u pretragu, svaki pritisak
 * tastera je događaj, a mi za svaki želimo predloge sa servera - ali:
 * - NE za svako slovo (čekaj da prestane da kuca) -> debounce/sample;
 * - NE prikazuj zastareo odgovor (kucao je dalje dok je stari leteo)
 *   -> switchMap otkazuje prethodni in-flight poziv.
 * <p>
 * Ključ je razlika flatMap vs switchMap:
 * - flatMap   pušta SVE inner-pozive paralelno; zastareli odgovori
 *             mogu da stignu posle novih (race, "treperenje" rezultata).
 * - switchMap pri svakom novom ulazu OTKAZUJE prethodni inner-Mono i
 *             pretplati se na novi. Uvek vidiš samo rezultat za
 *             POSLEDNJI upit. Tačno ono što typeahead traži.
 */
public class TypeaheadDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. Pritisci tastera kao Flux (simulacija kucanja) ===\n");
        primer1_kucanjeKaoFlux();

        System.out.println("\n=== 2. flatMap - ZASTARELI odgovori mogu da prestignu nove (loše) ===\n");
        primer2_flatMapProblem();

        System.out.println("\n=== 3. switchMap - otkazuje stari poziv, samo poslednji upit (dobro) ===\n");
        primer3_switchMap();

        System.out.println("\n=== 4. Pun typeahead: sampleTimeout + distinct + filter + switchMap ===\n");
        primer4_punTypeahead();
    }

    // -------------------------------------------------------------------
    // Simulacija kucanja: korisnik unosi "r", "re", "rea", "reac",
    // "react" sa neravnomernim pauzama. delayElements daje "ljudski"
    // tajming između pritisaka.
    // -------------------------------------------------------------------
    static Flux<String> kucanje() {
        return Flux.just("r", "re", "rea", "reac", "react")
                .delayElements(Duration.ofMillis(120));
    }

    static void primer1_kucanjeKaoFlux() throws InterruptedException {
        kucanje().subscribe(q -> log("keystroke", "'" + q + "'"));
        Thread.sleep(800);
    }

    // -------------------------------------------------------------------
    // Servtraži: simulira poziv ka /search?q=... Trik: KRAĆI upiti
    // namerno traju DUŽE (200ms) od dužih (40ms), da reprodukujemo
    // race - odgovor za "re" stiže POSLE odgovora za "react".
    // -------------------------------------------------------------------
    static Mono<List<String>> pretraga(String q) {
        int latencija = q.length() <= 2 ? 200 : 40;     // namerno: kratak upit = spor odgovor
        return Mono.just(List.of(q + "-rezultat-1", q + "-rezultat-2"))
                .delayElement(Duration.ofMillis(latencija));
    }

    // -------------------------------------------------------------------
    // flatMap: svaki keystroke pokrene poziv i svi teku paralelno.
    // Pošto je "re" sporo (200ms), njegov odgovor stigne NAKON odgovora
    // za "react". UI bi na kraju prikazao predloge za "re" iako korisnik
    // vidi "react" u polju - vizuelni bug.
    // -------------------------------------------------------------------
    static void primer2_flatMapProblem() throws InterruptedException {
        kucanje()
                .flatMap(TypeaheadDemo::pretraga)
                .subscribe(r -> log("flatMap", r));
        Thread.sleep(900);
        log("info", "primeti: stari upiti mogu da stignu poslednji -> pogrešan prikaz");
    }

    // -------------------------------------------------------------------
    // switchMap: čim stigne novi keystroke, prethodni in-flight poziv
    // se OTKAZUJE (cancel). Pošto pauza između slova (120ms) prekida
    // spore pozive (200ms), vidimo rezultat samo za poslednji upit.
    // -------------------------------------------------------------------
    static void primer3_switchMap() throws InterruptedException {
        kucanje()
                .switchMap(TypeaheadDemo::pretraga)
                .subscribe(r -> log("switchMap", r));
        Thread.sleep(900);
        log("info", "stari pozivi otkazani -> uvek samo aktuelni upit");
    }

    // -------------------------------------------------------------------
    // Produkcijski typeahead pipeline, korak po korak:
    //
    //   sampleTimeout(d)      - emituj upit tek kad korisnik napravi
    //                           pauzu od d (debounce - ne gađaj server
    //                           za svako slovo). Reactor naziv je
    //                           sampleTimeout: "uzmi poslednju vrednost
    //                           ako u sledećih d ništa nije stiglo".
    //   distinctUntilChanged  - ne traži dva puta isti string zaredom.
    //   filter(len >= 2)      - ignoriši prekratke upite ("r").
    //   switchMap(search)     - otkaži zastarele pozive.
    // -------------------------------------------------------------------
    static void primer4_punTypeahead() throws InterruptedException {
        // Brže kucanje "j","ja","jav","java", pa pauza, pa "javas".
        Flux<String> ulaz = Flux.concat(
                Flux.just("j", "ja", "jav", "java").delayElements(Duration.ofMillis(60)),
                Flux.just("javas").delayElements(Duration.ofMillis(400)));

        ulaz
                .sampleTimeout(q -> Mono.delay(Duration.ofMillis(150)))  // debounce 150ms
                .distinctUntilChanged()
                .filter(q -> q.length() >= 2)
                .switchMap(q -> {
                    log("->server", "'" + q + "'");
                    return pretraga(q);
                })
                .subscribe(r -> log("predlozi", r));

        Thread.sleep(1400);
        log("info", "server pogođen samo za stabilizovane upite, ne za svako slovo");
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
