package raf.edu.week2;

import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

/**
 * Nedelja 2 — osnovni operatori koji se PREKLAPAJU sa Stream API-jem.
 *
 * Glavna pouka: NE učimo nove pojmove, samo nove kontekste. Operatori
 * map / filter / take / skip / distinct / count / reduce / collectList
 * imaju identičnu semantiku kao u Stream-u — samo asinhronu.
 *
 * Demo paralelno pokazuje Stream verziju i Flux verziju, da se vidi
 * jedan-na-jedan preslikavanje.
 *
 * Pokriva i one operatore koji POSTOJE u Reactor-u, a ne i u Stream-u
 * (jer su prirodni za reaktivni svet):
 *   - distinctUntilChanged
 *   - scan (kao reduce, ali emituje međurezultate)
 */
public class OsnovniOperatori {

    public static void main(String[] args) {
        System.out.println("=== 1. map / filter — najčešći par ===\n");
        mapFilter();

        System.out.println("\n=== 2. take / skip ===\n");
        takeSkip();

        System.out.println("\n=== 3. distinct / distinctUntilChanged ===\n");
        distinct();

        System.out.println("\n=== 4. count / any / all ===\n");
        countAnyAll();

        System.out.println("\n=== 5. reduce vs scan ===\n");
        reduceVsScan();

        System.out.println("\n=== 6. collectList / collectMap ===\n");
        collect();

        System.out.println("\n=== 7. sort ===\n");
        sort();
    }

    // -------------------------------------------------------------------
    // map = po-element transformacija (T → R)
    // filter = po-element predikat (T → boolean)
    // -------------------------------------------------------------------
    static void mapFilter() {
        // Stream verzija
        List<Integer> izStream = List.of(1, 2, 3, 4, 5).stream()
                .filter(n -> n % 2 == 1)
                .map(n -> n * n)
                .toList();
        System.out.println("  [Stream]  " + izStream);

        // Flux verzija — IDENTIČAN pipeline
        Flux.just(1, 2, 3, 4, 5)
                .filter(n -> n % 2 == 1)
                .map(n -> n * n)
                .collectList()
                .subscribe(rez -> System.out.println("  [Flux]    " + rez));
    }

    // -------------------------------------------------------------------
    // take(n) — uzmi prvih n elemenata, pa onComplete.
    // skip(n) — preskoči prvih n.
    // (Postoje i vremenske varijante — videti VremenskiOperatori.)
    // -------------------------------------------------------------------
    static void takeSkip() {
        Flux.range(1, 10)
                .take(3)
                .collectList()
                .subscribe(r -> System.out.println("  [take(3)]  " + r));

        Flux.range(1, 10)
                .skip(7)
                .collectList()
                .subscribe(r -> System.out.println("  [skip(7)]  " + r));

        Flux.range(1, 10)
                .skip(3)
                .take(4)
                .collectList()
                .subscribe(r -> System.out.println("  [skip(3).take(4)] " + r));
    }

    // -------------------------------------------------------------------
    // distinct — bez ijednog ponovljenog elementa u celom toku.
    // distinctUntilChanged — bez UZASTOPNO ponovljenog. Razlika je važna
    //   za npr. praćenje promene stanja: ako vrednost ostane ista,
    //   ne re-emituj.
    // -------------------------------------------------------------------
    static void distinct() {
        Flux.just(1, 2, 2, 3, 3, 3, 1, 4)
                .distinct()
                .collectList()
                .subscribe(r -> System.out.println("  [distinct]              " + r));

        Flux.just(1, 2, 2, 3, 3, 3, 1, 4)
                .distinctUntilChanged()
                .collectList()
                .subscribe(r -> System.out.println("  [distinctUntilChanged]  " + r));
    }

    // -------------------------------------------------------------------
    // count / any / all — terminali koji vraćaju Mono.
    // -------------------------------------------------------------------
    static void countAnyAll() {
        Flux.range(1, 10)
                .count()
                .subscribe(n -> System.out.println("  [count]    " + n));

        Flux.range(1, 10)
                .any(n -> n > 5)
                .subscribe(b -> System.out.println("  [any > 5]  " + b));

        Flux.range(1, 10)
                .all(n -> n > 0)
                .subscribe(b -> System.out.println("  [all > 0]  " + b));
    }

    // -------------------------------------------------------------------
    // reduce — emituje SAMO finalnu vrednost.
    // scan   — emituje SVAKI međurezultat (prirodno za running totals).
    //          Ovaj operator NE postoji na Stream-u.
    // -------------------------------------------------------------------
    static void reduceVsScan() {
        Flux.range(1, 5)
                .reduce(0, Integer::sum)
                .subscribe(n -> System.out.println("  [reduce]   finalna suma = " + n));

        System.out.println("  [scan]     međurezultati:");
        Flux.range(1, 5)
                .scan(0, Integer::sum)
                .subscribe(n -> System.out.println("              " + n));
    }

    // -------------------------------------------------------------------
    // collectList / collectMap — sakupi elemente u kolekciju i vrati Mono.
    // -------------------------------------------------------------------
    static void collect() {
        Flux.just("Ana", "Marko", "Petar")
                .collectList()
                .subscribe(list -> System.out.println("  [collectList]  " + list));

        Flux.just("Ana", "Marko", "Petar")
                .collectMap(String::length)
                .subscribe((Map<Integer, String> m) -> System.out.println("  [collectMap]   " + m));
    }

    // -------------------------------------------------------------------
    // sort — radi tek kada izvor završi (mora se sve videti pre sortiranja).
    // -------------------------------------------------------------------
    static void sort() {
        Flux.just(5, 2, 8, 1, 9, 3)
                .sort()
                .collectList()
                .subscribe(r -> System.out.println("  [sort]              " + r));

        Flux.just(5, 2, 8, 1, 9, 3)
                .sort((a, b) -> b - a)   // descending
                .collectList()
                .subscribe(r -> System.out.println("  [sort(desc)]        " + r));
    }
}
