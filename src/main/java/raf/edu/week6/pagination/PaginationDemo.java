package raf.edu.week6.pagination;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Nedelja 6 - paginacija API-ja preko expand().
 * <p>
 * Realan REST API retko vrati sve odjednom - vraća STRANICU plus
 * pokazivač na sledeću ("nextPage", "cursor", "Link: rel=next").
 * Problem: ne znamo unapred koliko stranica ima, pa ne možemo
 * Flux.range(1, N). Treba nam rekurzija: "povuci stranicu, ako ima
 * sledeća povuci i nju, dok se ne potroše".
 * <p>
 * expand() je tačno taj operator: za svaki emitovani element pozove
 * funkciju koja vraća Publisher SLEDEĆIH elemenata, pa to ponovi
 * rekurzivno dok funkcija ne vrati prazno. (BFS po stablu; ovde je
 * stablo linearno: stranica -> sledeća stranica.)
 * <p>
 * Bonus prednost reaktivnog modela: lenjo je. Sa takeUntil/take
 * možemo prestati ranije - i expand NEĆE povući ostatak stranica.
 */
public class PaginationDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static final int UKUPNO_STRANICA = 5;
    private static final int PO_STRANICI = 3;

    // -------------------------------------------------------------------
    // Jedna stranica odgovora: stavke + broj sledeće stranice (null ako
    // je ovo poslednja). Tačno oblik koji vraća paginated REST API.
    // -------------------------------------------------------------------
    record Page(int broj, List<Integer> stavke, Integer sledeca) {
        boolean imaSledecu() {
            return sledeca != null;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. Jedan poziv = jedna stranica ===\n");
        primer1_jednaStranica();

        System.out.println("\n=== 2. expand() - povuci SVE stranice rekurzivno ===\n");
        primer2_sveStranice();

        System.out.println("\n=== 3. expand() + takeUntil - stani čim nađeš dovoljno (lenjo) ===\n");
        primer3_ranoZaustavljanje();
    }

    // -------------------------------------------------------------------
    // Simulira GET /items?page=N. Latencija 80ms po stranici. Poslednja
    // stranica ima sledeca == null.
    // -------------------------------------------------------------------
    static Mono<Page> fetchPage(int broj) {
        List<Integer> stavke = IntStream.range(0, PO_STRANICI)
                .map(i -> (broj - 1) * PO_STRANICI + i + 1)
                .boxed()
                .toList();
        Integer sledeca = broj < UKUPNO_STRANICA ? broj + 1 : null;
        return Mono.just(new Page(broj, stavke, sledeca))
                .delayElement(Duration.ofMillis(80))
                .doOnSubscribe(s -> log("GET", "/items?page=" + broj));
    }

    static void primer1_jednaStranica() {
        Page p = fetchPage(1).block();
        log("page", p);
    }

    // -------------------------------------------------------------------
    // expand(): kreni od prve stranice, pa za svaku vraćaj sledeću dok
    // ne dođeš do one bez "sledeca". Onda flatMap stavke u jedan tok.
    //
    //   fetchPage(1) --expand--> fetchPage(2) --expand--> ... --> kraj
    //
    // Rezultat: Flux<Page> svih stranica, redom. concatMapIterable ih
    // razloži u Flux<Integer> svih stavki.
    // -------------------------------------------------------------------
    static void primer2_sveStranice() {
        List<Integer> sve = fetchPage(1)
                .expand(page -> page.imaSledecu()
                        ? fetchPage(page.sledeca())
                        : Mono.empty())                 // prazno = kraj rekurzije
                .concatMapIterable(Page::stavke)        // Flux<Page> -> Flux<Integer>
                .collectList()
                .block();

        log("ukupno", sve.size() + " stavki: " + sve);
    }

    // -------------------------------------------------------------------
    // Lenjost u akciji: hoćemo prvih 7 stavki. takeUntil zaustavi tok
    // čim akumuliramo dovoljno - expand tad NE povlači preostale
    // stranice (vidi se po izostalom "GET /items?page=4,5" logu).
    //
    // Ovo je velika prednost nad imperativnim "učitaj sve pa iseci".
    // -------------------------------------------------------------------
    static void primer3_ranoZaustavljanje() {
        List<Integer> prvih7 = fetchPage(1)
                .expand(page -> page.imaSledecu()
                        ? fetchPage(page.sledeca())
                        : Mono.empty())
                .concatMapIterable(Page::stavke)
                .take(7)                                // dovoljno -> otkaži ostatak
                .collectList()
                .block();

        log("prvih7", prvih7);
        log("info", "GET za page=4 i page=5 NIJE pozvan - expand stao na vreme");
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
