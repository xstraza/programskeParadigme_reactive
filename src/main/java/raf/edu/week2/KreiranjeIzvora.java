package raf.edu.week2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 2 — kreiranje izvora u Project Reactor-u.
 *
 * Reactor ima desetine factory metoda. Ovaj demo grupiše glavne po
 * vrsti izvora:
 *   1. Statičke vrednosti           — just, empty, error, never
 *   2. Iz Java struktura             — fromIterable, fromArray, fromStream, range
 *   3. Lenji izvori                  — fromCallable, fromSupplier, defer
 *   4. Iz CompletableFuture          — fromFuture
 *   5. Vremenski izvori              — interval, delay
 *   6. Programsko kreiranje          — generate (sinhroni), create (async)
 *
 * Glavna pouka: NE postoji "factory metoda za sve". Bira se prema
 * tome šta je izvor podataka — i da li ti treba lenjost.
 */
public class KreiranjeIzvora {

    public static void main(String[] args) {
        System.out.println("=== 1. Statičke vrednosti ===\n");
        statickeVrednosti();

        System.out.println("\n=== 2. Iz Java struktura ===\n");
        izJavaStruktura();

        System.out.println("\n=== 3. Lenji izvori (fromCallable, defer) ===\n");
        lenjiIzvori();

        System.out.println("\n=== 4. Iz CompletableFuture ===\n");
        izFuture();

        System.out.println("\n=== 5. Vremenski izvori ===\n");
        vremenskiIzvori();

        System.out.println("\n=== 6. Programski (generate, create) ===\n");
        programskoKreiranje();
    }

    // -------------------------------------------------------------------
    // 1. just / empty / error / never — kad imamo vrednost(i) već u ruci.
    // -------------------------------------------------------------------
    static void statickeVrednosti() {
        Mono.just("zdravo").subscribe(v -> System.out.println("  [just]    " + v));
        Mono.justOrEmpty(maybeValue(true))
                .subscribe(v -> System.out.println("  [justOrEmpty/ima]    " + v),
                           err -> {},
                           () -> System.out.println("  [justOrEmpty/ima]    onComplete"));
        Mono.justOrEmpty(maybeValue(false))
                .subscribe(v -> System.out.println("  [justOrEmpty/null]   " + v),
                           err -> {},
                           () -> System.out.println("  [justOrEmpty/null]   onComplete (prazan)"));

        Flux.just(1, 2, 3).subscribe(v -> System.out.println("  [Flux.just] " + v));

        Mono.error(new IllegalStateException("simulirana greska"))
                .subscribe(v -> {},
                           err -> System.out.println("  [error]   onError: " + err.getMessage()));
    }

    static String maybeValue(boolean ima) {
        return ima ? "neka-vrednost" : null;
    }

    // -------------------------------------------------------------------
    // 2. fromIterable / fromArray / fromStream / range — kad već imamo
    //    Java strukturu i samo treba "obući" je u Flux.
    // -------------------------------------------------------------------
    static void izJavaStruktura() {
        List<String> imena = List.of("Ana", "Marko", "Petar");
        Flux.fromIterable(imena)
                .subscribe(v -> System.out.println("  [fromIterable] " + v));

        Integer[] niz = {10, 20, 30};
        Flux.fromArray(niz)
                .subscribe(v -> System.out.println("  [fromArray]    " + v));

        Flux.fromStream(imena.stream().map(String::toUpperCase))
                .subscribe(v -> System.out.println("  [fromStream]   " + v));

        // OPREZ: range(1, 5) je 1..5 (count, ne endIndex!)
        Flux.range(1, 5)
                .subscribe(v -> System.out.println("  [range(1,5)]   " + v));
    }

    // -------------------------------------------------------------------
    // 3. fromCallable / fromSupplier / defer — kad izvor zavisi od
    //    TRENUTKA subscribe-a (vreme, random, baza, ...).
    //
    // Razlika prema just():
    //   - just(x)            — x se izračuna ODMAH, kad se konstruiše Mono
    //   - fromCallable(...)  — izračun se desi tek pri svakom subscribe
    // -------------------------------------------------------------------
    static void lenjiIzvori() {
        // Brojač — povećava se na svaki "rad".
        AtomicInteger brojac = new AtomicInteger();

        Mono<Integer> eager = Mono.just(brojac.incrementAndGet());
        // ^ poziv je VEC izvršen — brojač je 1, fiksiran zauvek.

        Mono<Integer> lazy = Mono.fromCallable(brojac::incrementAndGet);
        // ^ poziv se izvršava na svakom subscribe-u — brojač raste.

        System.out.println("  [eager #1] " + eager.block());
        System.out.println("  [eager #2] " + eager.block());
        System.out.println("  [eager #3] " + eager.block());

        System.out.println("  [lazy  #1] " + lazy.block());
        System.out.println("  [lazy  #2] " + lazy.block());
        System.out.println("  [lazy  #3] " + lazy.block());

        // defer — ide jos dalje, omotava CEO konstrukt Mono-a.
        // Korisno kad je sam Mono "skup" da se sastavi (npr. otvara konekciju).
        Mono<String> deferran = Mono.defer(() -> {
            System.out.println("    (defer: gradim Mono...)");
            return Mono.just("vrednost-" + System.nanoTime());
        });

        System.out.println("  [defer #1] " + deferran.block());
        System.out.println("  [defer #2] " + deferran.block());
    }

    // -------------------------------------------------------------------
    // 4. Mono.fromFuture — premostavanje sa CompletableFuture-om.
    //    U realnom kodu vrlo često zatrebatreba: postojeća async biblioteka
    //    vraća CompletableFuture, mi je "obučemo" u Mono i lančamo.
    // -------------------------------------------------------------------
    static void izFuture() {
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            return "rezultat-iz-future-a";
        });

        Mono<String> mono = Mono.fromFuture(future);

        // Sad je to reaktivni tip — možemo lančati operatore.
        mono.map(String::toUpperCase)
                .subscribe(v -> System.out.println("  [fromFuture] " + v));

        // block na main niti da se isprintaju rezultati pre exit-a.
        mono.block();
    }

    // -------------------------------------------------------------------
    // 5. Vremenski izvori.
    // -------------------------------------------------------------------
    static void vremenskiIzvori() {
        // interval — beskonačan tok 0, 1, 2, ... svakih 100ms.
        // Bez take() bi tek tako prazno tekao zauvek.
        Flux.interval(Duration.ofMillis(100))
                .take(3)
                .doOnNext(t -> System.out.println("  [interval] tick " + t
                        + " na " + Thread.currentThread().getName()))
                .blockLast();

        // Mono.delay — emituje 0L nakon datog vremena, pa onComplete.
        Long tek = Mono.delay(Duration.ofMillis(150)).block();
        System.out.println("  [delay] emisija: " + tek);
    }

    // -------------------------------------------------------------------
    // 6. Programsko kreiranje — generate i create.
    //
    // generate — sinhroni; jedna lambda, jedan element po pozivu.
    // create   — async; lambda dobija sink na koji guramo elemente
    //            kad nam stignu (npr. iz callback-a).
    // -------------------------------------------------------------------
    static void programskoKreiranje() {
        // generate — Fibonacci sa stanjem.
        Flux<Integer> fibonacci = Flux.generate(
                () -> new int[]{0, 1},                       // početno stanje
                (state, sink) -> {
                    sink.next(state[0]);                     // emituj jedan element
                    int sledeci = state[0] + state[1];
                    return new int[]{state[1], sledeci};     // vrati novo stanje
                });

        fibonacci.take(8)
                .subscribe(v -> System.out.print("  [generate-fib] " + v + "  "));
        System.out.println();

        // create — sinhroni primer (u praksi se koristi za async callback bridge).
        // Ovde guramo nekoliko elemenata u sink direktno.
        Flux<String> fromCreate = Flux.create(sink -> {
            sink.next("prvi");
            sink.next("drugi");
            sink.next("treci");
            sink.complete();
        });

        fromCreate.subscribe(v -> System.out.println("  [create]       " + v));
    }
}
