package raf.edu.week3;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 3 — merge, concat, mergeSequential.
 *
 * Sva tri operatora KOMBINUJU vise Flux-eva u jedan rezultujuci Flux.
 * Razlikuju se po REDOSLEDU emisija u rezultatu:
 *
 *   merge            — pretplati se na sve odmah, INTERLEAVE-uj rezultate
 *                      onako kako stizu kroz vreme. Paralelno.
 *
 *   concat           — pretplati se na PRVI, sacekaj njegov onComplete,
 *                      pa onda na sledeci, itd. Strogi redosled.
 *
 *   mergeSequential  — pretplati se na sve odmah (paralelno), ali bafera
 *                      rezultate i izlaze ih u redosledu IZVORA.
 *
 * Razlika izmedju merge/concat i flatMap/concatMap: ovi rade nad
 * VEC POSTOJECIM Flux-evima (Flux&lt;T&gt;, Flux&lt;T&gt;, ...), dok flatMap
 * dinamicki proizvodi unutrasnje tokove iz elemenata ulaza
 * (T -&gt; Flux&lt;R&gt;).
 *
 * U sustini:
 *   - merge(a, b, c)            ≡ Flux.just(a, b, c).flatMap(x -&gt; x)
 *   - concat(a, b, c)           ≡ Flux.just(a, b, c).concatMap(x -&gt; x)
 *   - mergeSequential(a, b, c)  ≡ Flux.just(a, b, c).flatMapSequential(x -&gt; x)
 */
public class MergeConcatDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. merge — paralelno, interleaving ===\n");
        mergeDemo();

        System.out.println("\n=== 2. concat — serijski, redosled izvora ===\n");
        concatDemo();

        System.out.println("\n=== 3. mergeSequential — paralelno + redosled izvora ===\n");
        mergeSequentialDemo();

        System.out.println("\n=== 4. mergeWith / concatWith — instance metode ===\n");
        instanceMethodsDemo();

        System.out.println("\n=== 5. startWith / concatWithValues — dodaj na pocetak/kraj ===\n");
        startWithDemo();

        System.out.println("\n=== 6. concat sa greskom — prekida lanac ===\n");
        concatErrorDemo();
    }

    // -------------------------------------------------------------------
    // merge — sva tri toka rade paralelno. Brzi toc pobegne sporom.
    //
    // Ovde fluxA emituje na svakih 100ms, fluxB na svakih 150ms.
    // Ispis: rezultati pomesani po vremenu.
    // -------------------------------------------------------------------
    static void mergeDemo() {
        Flux<String> fluxA = Flux.just("A1", "A2", "A3")
                .delayElements(Duration.ofMillis(100));
        Flux<String> fluxB = Flux.just("B1", "B2", "B3")
                .delayElements(Duration.ofMillis(150));

        Flux.merge(fluxA, fluxB)
                .doOnNext(v -> log("merge", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // concat — fluxB ne pocinje dok fluxA ne zavrsi.
    // Ispis: A1, A2, A3, B1, B2, B3 — bez obzira na to sto je B brzi
    // ili sporiji.
    // -------------------------------------------------------------------
    static void concatDemo() {
        Flux<String> fluxA = Flux.just("A1", "A2", "A3")
                .delayElements(Duration.ofMillis(100));
        Flux<String> fluxB = Flux.just("B1", "B2", "B3")
                .delayElements(Duration.ofMillis(50));

        Flux.concat(fluxA, fluxB)
                .doOnNext(v -> log("concat", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // mergeSequential — oba toka su odmah pokrenuta (paralelno), ali
    // izlazni redosled je: svi A pa svi B. Brzi B je "cekao" sve A da
    // izadju, iako su mu rezultati spremni ranije.
    //
    // Korisno kad zelimo paralelizam ali nam je redosled vazan.
    // -------------------------------------------------------------------
    static void mergeSequentialDemo() {
        Flux<String> fluxA = Flux.just("A1", "A2", "A3")
                .delayElements(Duration.ofMillis(150));
        Flux<String> fluxB = Flux.just("B1", "B2", "B3")
                .delayElements(Duration.ofMillis(50));

        Flux.mergeSequential(fluxA, fluxB)
                .doOnNext(v -> log("mergeSequential", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // mergeWith / concatWith — verzije gde se "ulancavanje" radi nad
    // postojecim Flux-om. Citljivije kad imamo glavni tok i jedan
    // dodatni izvor.
    // -------------------------------------------------------------------
    static void instanceMethodsDemo() {
        Flux<String> osnova = Flux.just("X1", "X2");
        Flux<String> dodatak = Flux.just("Y1", "Y2");

        osnova.mergeWith(dodatak)
                .doOnNext(v -> log("mergeWith", v))
                .blockLast();

        osnova.concatWith(dodatak)
                .doOnNext(v -> log("concatWith", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // startWith — ubaci vrednost(i) na POCETAK toka.
    // concatWithValues — analog na kraju.
    //
    // Cesto se koristi za "init" vrednost (npr. UI state koji prvo
    // mora da emituje "loading" pa onda prave podatke).
    // -------------------------------------------------------------------
    static void startWithDemo() {
        Flux.just("podaci1", "podaci2")
                .startWith("loading")
                .concatWithValues("done")
                .doOnNext(v -> log("startWith", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // PAZNJA: concat PREKIDA lanac na prvi error. Ako fluxA pukne,
    // fluxB se nikada nece ni pretplatiti.
    //
    // merge se ponasa slicno (default), ali ima i mergeDelayError koji
    // odlozi gresku da ne prekine ostale tokove. Za detalje o error
    // handling-u — nedelja 5.
    // -------------------------------------------------------------------
    static void concatErrorDemo() {
        Flux<String> fluxA = Flux.concat(
                Flux.just("A1"),
                Flux.error(new RuntimeException("pukao A")));
        Flux<String> fluxB = Flux.just("B1", "B2");

        Flux.concat(fluxA, fluxB)
                .doOnNext(v -> log("concat", v))
                .doOnError(e -> log("error", e.getMessage()))
                .onErrorResume(e -> Flux.empty())   // da block ne padne
                .blockLast();

        log("napomena", "B1, B2 se NIKAD nisu emitovali jer je A pukao");
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
