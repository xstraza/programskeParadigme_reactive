package raf.edu.week2;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Nedelja 2 - rešenja zadataka iz {@link PracticeTasksForStudents}.
 *
 * Svako rešenje je u zasebnoj statičkoj metodi i može se pokrenuti
 * pojedinačno.
 */
public class PracticeTasksSolutions {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== Zadatak 1 ===");
        zadatak1();
        System.out.println("\n=== Zadatak 2 ===");
        zadatak2();
        System.out.println("\n=== Zadatak 3 ===");
        zadatak3();
        System.out.println("\n=== Zadatak 4 ===");
        zadatak4();
        System.out.println("\n=== Zadatak 5 ===");
        zadatak5();
        System.out.println("\n=== Zadatak 6 ===");
        zadatak6();
        System.out.println("\n=== Zadatak 7 ===");
        zadatak7();
        System.out.println("\n=== Zadatak 8 ===");
        zadatak8();
    }

    // -------------------------------------------------------------------
    // Zadatak 1 - Title Case + filter na 4+ karaktera.
    //
    // Tehnike: fromIterable, map (sa pomocnom funkcijom), filter, subscribe.
    // -------------------------------------------------------------------
    static void zadatak1() {
        Flux.fromIterable(List.of("Ana", "marko", "PETAR", "Jovana"))
                .map(PracticeTasksSolutions::titleCase)
                .filter(s -> s.length() >= 4)
                .subscribe(System.out::println);
    }

    static String titleCase(String s) {
        if (s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }

    // -------------------------------------------------------------------
    // Zadatak 2 - suma kvadrata neparnih iz 1..20.
    //
    // Tehnike: range, filter, map, reduce, subscribe.
    // -------------------------------------------------------------------
    static void zadatak2() {
        Flux.range(1, 20)
                .filter(n -> n % 2 == 1)
                .map(n -> n * n)
                .reduce(0, Integer::sum)
                .subscribe(suma -> System.out.println("Suma = " + suma));
    }

    // -------------------------------------------------------------------
    // Zadatak 3 - eager just vs lazy fromCallable.
    //
    // just  : vrednost se izračuna PRI kreiranju Mono-a; sva 3 ista.
    // fromCallable : izračun na svaki subscribe; 3 različita vremena.
    // -------------------------------------------------------------------
    static void zadatak3() {
        Mono<Long> eager = Mono.just(System.currentTimeMillis());
        Mono<Long> lazy = Mono.fromCallable(System::currentTimeMillis);

        System.out.println("eager:");
        for (int i = 0; i < 3; i++) {
            System.out.println("  " + eager.block());
            spavaj(50);
        }

        System.out.println("lazy:");
        for (int i = 0; i < 3; i++) {
            System.out.println("  " + lazy.block());
            spavaj(50);
        }
    }

    // -------------------------------------------------------------------
    // Zadatak 4 - vremenski razvučen tok.
    //
    // Tehnike: range, delayElements, doOnNext sa logom, blockLast.
    //
    // Napomena za studente: nit je iz Schedulers.parallel() pool-a
    // (parallel-N) jer delayElements podrazumevano koristi taj scheduler.
    // O Schedulers-ima više u nedelji 4.
    // -------------------------------------------------------------------
    static void zadatak4() {
        Flux.range(1, 5)
                .delayElements(Duration.ofMillis(300))
                .doOnNext(n -> System.out.printf("  [%s] (%s) -> %d%n",
                        LocalTime.now().format(HHMMSS),
                        Thread.currentThread().getName(),
                        n))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 5 - svi doOn* hooks redom.
    //
    // Redosled koji vidimo:
    //   subscribe -> request -> next(a) -> next(b) -> next(c)
    //             -> complete -> finally
    // -------------------------------------------------------------------
    static void zadatak5() {
        Flux.just("a", "b", "c")
                .doOnSubscribe(sub -> System.out.println("doOnSubscribe"))
                .doOnRequest(n     -> System.out.println("doOnRequest: " + n))
                .doOnNext(v        -> System.out.println("doOnNext: " + v))
                .doOnComplete(()   -> System.out.println("doOnComplete"))
                .doFinally(sig     -> System.out.println("doFinally: " + sig))
                .subscribe();
    }

    // -------------------------------------------------------------------
    // Zadatak 6 - error tok, defaultIfEmpty vs onErrorReturn.
    //
    // POENTA: defaultIfEmpty se okida SAMO na onComplete bez emisije.
    // Na error tok ostaje "pukao". Za fallback na error koristi se
    // onErrorReturn (ili onErrorResume - week 5).
    // -------------------------------------------------------------------
    static void zadatak6() {
        // (a)+(b) error + log
        System.out.println("(a)+(b) error sa log:");
        try {
            Mono.<String>error(new RuntimeException("oops"))
                    .log("err")
                    .block();
        } catch (Exception e) {
            System.out.println("  block bacio: " + e.getMessage());
        }

        // (c) defaultIfEmpty NE pomaže ako tok pukne (samo ako bude prazan).
        System.out.println("\n(c) defaultIfEmpty + error -> i dalje pukne:");
        try {
            Mono.<String>error(new RuntimeException("oops"))
                    .defaultIfEmpty("default")
                    .block();
        } catch (Exception e) {
            System.out.println("  block bacio: " + e.getMessage());
        }

        // (d) onErrorReturn - to JE fallback za error.
        System.out.println("\n(d) onErrorReturn - sad smo zaštićeni:");
        String v = Mono.<String>error(new RuntimeException("oops"))
                .onErrorReturn("fallback")
                .block();
        System.out.println("  vrednost: " + v);
    }

    // -------------------------------------------------------------------
    // Zadatak 7 - fallback chain (kes -> baza -> API).
    //
    // Tehnike: Mono.empty / Mono.fromCallable, switchIfEmpty, doOnNext.
    // -------------------------------------------------------------------
    static void zadatak7() {
        nadjiUKesu()
                .doOnNext(v -> System.out.println("  iz kesa: " + v))
                .switchIfEmpty(nadjiUBazi()
                        .doOnNext(v -> System.out.println("  iz baze: " + v)))
                .switchIfEmpty(nadjiPrekoApija()
                        .doOnNext(v -> System.out.println("  iz API-ja: " + v)))
                .subscribe(v -> System.out.println("Konačno: " + v));
    }

    static Mono<String> nadjiUKesu() {
        System.out.println("    -> proverim kes");
        return Mono.empty();
    }

    static Mono<String> nadjiUBazi() {
        System.out.println("    -> proverim bazu");
        return Mono.empty();
    }

    static Mono<String> nadjiPrekoApija() {
        System.out.println("    -> zovnem API");
        return Mono.fromCallable(() -> "Marko Marković");
    }

    // -------------------------------------------------------------------
    // Zadatak 8 - Fibonacci do 100, pomoću Flux.generate.
    //
    // Tehnike: generate sa stanjem (par [a, b]), sink.next, sink.complete.
    // -------------------------------------------------------------------
    static void zadatak8() {
        Flux<Integer> fibonacci = Flux.generate(
                () -> new int[]{1, 1},
                (state, sink) -> {
                    if (state[0] > 100) {
                        sink.complete();
                    } else {
                        sink.next(state[0]);
                    }
                    return new int[]{state[1], state[0] + state[1]};
                });

        List<Integer> rez = fibonacci.collectList().block();
        System.out.println(rez);
    }

    // -------------------------------------------------------------------
    // Mali helper za zadatak 3 - sleep bez interrupted exception buke.
    // -------------------------------------------------------------------
    static void spavaj(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }
}
