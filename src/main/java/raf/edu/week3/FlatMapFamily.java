package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 3 - šira porodica flatMap-a.
 *
 * Pored flatMap / concatMap / flatMapSequential (videti
 * {@link FlatMapVariants}), Reactor ima još tri operatora koja u
 * istoj familiji rešavaju specifične potrebe:
 *
 *   flatMapMany       - Mono<T> → Flux<R>. "Jedan element u mnogo."
 *                       Tipično: jedan HTTP poziv vrati listu, hoću
 *                       svaki red kao zaseban Flux element.
 *
 *   flatMapIterable   - Flux<T> sa T = Iterable<R>, daje Flux<R>.
 *                       Brži i citkiji nego flatMap(x -&gt; Flux.fromIterable(x)).
 *                       Bez asinhronog "spinanja" unutrasnjeg toka.
 *
 *   expand            - rekurzivno proširivanje. Za svaki emitovani
 *                       element pokrene novi Publisher, a njegove
 *                       rezultate ponovo "razgranja" - dok ne stane.
 *                       Klasičan primer: PAGINACIJA, obilazak stabla.
 */
public class FlatMapFamily {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. flatMapMany - Mono → Flux ===\n");
        flatMapManyDemo();

        System.out.println("\n=== 2. flatMapIterable - Flux<List<T>> → Flux<T> ===\n");
        flatMapIterableDemo();

        System.out.println("\n=== 3. expand - paginacija API-ja ===\n");
        expandPaginationDemo();

        System.out.println("\n=== 4. expand - BFS obilazak (kategorije sa pod-kategorijama) ===\n");
        expandTreeDemo();
    }

    // -------------------------------------------------------------------
    // flatMapMany - Mono na ulazu daje Flux. "Jedan request, vise redova."
    //
    // Bez flatMapMany morali bismo prvo .flatMap pa onda nekako da
    // pretvorimo unutrasnji tip - flatMapMany to radi u jednom koraku.
    // -------------------------------------------------------------------
    static void flatMapManyDemo() {
        Mono<String> jednoIme = Mono.just("Ana,Marko,Petar");

        jednoIme
                .flatMapMany(csv -> Flux.fromArray(csv.split(",")))
                .doOnNext(v -> log("flatMapMany", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // flatMapIterable - kad u Flux-u imamo elemente cija je svaka
    // vrednost Iterable, i hocemo "ravno" prelivanje.
    //
    // Primer: API vraca stranice (List<Item> po stranici); hocemo
    // Flux<Item> bez ugnjezdjavanja.
    // -------------------------------------------------------------------
    static void flatMapIterableDemo() {
        Flux<List<Integer>> stranice = Flux.just(
                List.of(1, 2, 3),
                List.of(4, 5),
                List.of(6, 7, 8, 9));

        stranice
                .flatMapIterable(stranica -> stranica)
                .doOnNext(v -> log("flatMapIterable", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // expand - REKURZIVNO prosirivanje. Klasicno za paginaciju:
    //
    //   1. fetch(page=0) → emit Page0
    //   2. emit Page0 trigeruje expander → fetch(page=1) → emit Page1
    //   3. itd.
    //   4. stane kada expander vrati Mono.empty()
    //
    // U downstream izlazi ceo niz stranica.
    //
    // Ovde simuliramo API koji ima 4 stranice (0..3). Posle stranice 3
    // nema vise (vraca Mono.empty), pa expand staje.
    // -------------------------------------------------------------------
    static void expandPaginationDemo() {
        Mono<Stranica> prva = fetchStranica(0);

        prva
                .expand(s -> s.imaSledecu()
                        ? fetchStranica(s.brojStranice() + 1)
                        : Mono.empty())
                .doOnNext(s -> log("expand", s))
                .blockLast();
    }

    record Stranica(int brojStranice, List<Integer> stavke, boolean imaSledecu) {}

    static Mono<Stranica> fetchStranica(int br) {
        log("fetch", "stranica " + br);
        return Mono.just(new Stranica(
                        br,
                        List.of(br * 10, br * 10 + 1, br * 10 + 2),
                        br < 3))
                .delayElement(Duration.ofMillis(100));
    }

    // -------------------------------------------------------------------
    // expand za stablo - BFS obilazak. Svaki cvor expander vrati
    // listu svoje dece kao Flux. Idiom za rekurzivnu strukturu bez
    // rucne implementacije BFS-a.
    //
    // Korenovi:
    //   1
    //     ├ 2
    //     │   ├ 4
    //     │   └ 5
    //     └ 3
    //         └ 6
    //
    // Output (BFS): 1, 2, 3, 4, 5, 6
    // -------------------------------------------------------------------
    static void expandTreeDemo() {
        Flux.just(1)
                .expand(cvor -> Flux.fromIterable(deca(cvor)))
                .doOnNext(v -> log("expand-bfs", v))
                .blockLast();
    }

    static List<Integer> deca(int n) {
        return switch (n) {
            case 1 -> List.of(2, 3);
            case 2 -> List.of(4, 5);
            case 3 -> List.of(6);
            default -> List.of();
        };
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
