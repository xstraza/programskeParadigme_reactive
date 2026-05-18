package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Nedelja 3 - rešenja zadataka iz {@link PracticeTasksForStudents}.
 *
 * Svako rešenje je u zasebnoj statičkoj metodi i može se pokrenuti
 * pojedinačno (iz main-a su sva otkomentarisana, pa main sve odjednom
 * pokrene; jednostavno zakomentariši ono što ne želiš).
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
        System.out.println("\n=== Zadatak 9 ===");
        zadatak9();
        System.out.println("\n=== Zadatak 10 ===");
        zadatak10();
    }

    // -------------------------------------------------------------------
    // Zadatak 1 - flatMap sa Mono. Redosled NIJE garantovan (svi pozivi
    // imaju isti delay pa ce praktično pristići po redu, ali to nije
    // semanticka garancija flatMap-a).
    // -------------------------------------------------------------------
    static void zadatak1() {
        Flux.just(1, 2, 3, 4, 5)
                .flatMap(PracticeTasksSolutions::fetchUser)
                .doOnNext(System.out::println)
                .blockLast();
    }

    static Mono<String> fetchUser(int id) {
        return Mono.just("User#" + id).delayElement(Duration.ofMillis(100));
    }

    // -------------------------------------------------------------------
    // Zadatak 2 - concatMap (redosled garantovan).
    //
    // Sa nasumicnim trajanjem se vidi razlika - flatMap bi pomesao
    // redosled, concatMap ga drži.
    // -------------------------------------------------------------------
    static void zadatak2() {
        Flux.just(1, 2, 3, 4, 5)
                .concatMap(id -> Mono.just("User#" + id)
                        .delayElement(Duration.ofMillis(ThreadLocalRandom.current().nextInt(50, 300))))
                .doOnNext(System.out::println)
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 3 - switchMap (autocomplete).
    //
    // Razmak izmedju karaktera (50ms) je MNOGO manji od trajanja
    // unutrasnjeg poziva (200ms), pa switchMap otkazuje sve osim
    // poslednjeg.
    // -------------------------------------------------------------------
    static void zadatak3() {
        Flux.just("B", "Be", "Beo", "Beog")
                .delayElements(Duration.ofMillis(50))
                .switchMap(upit -> Mono.just("rezultat za " + upit)
                        .delayElement(Duration.ofMillis(200))
                        .doOnSubscribe(s -> log("start", upit))
                        .doOnCancel(()  -> log("cancel", upit)))
                .doOnNext(v -> log("rezultat", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 4 - Mono.zip kao Promise.all.
    //
    // Mereno vreme treba da bude ~max(300, 500, 200) = 500ms, ne 1000ms.
    // -------------------------------------------------------------------
    static void zadatak4() {
        long t0 = System.currentTimeMillis();

        String dashboard = Mono.zip(profile(), posts(), friends())
                .map(t -> "Dashboard: " + t.getT1() + " + " + t.getT2() + " + " + t.getT3())
                .block();

        long elapsed = System.currentTimeMillis() - t0;
        System.out.println(dashboard);
        System.out.println("  ukupno trajanje: " + elapsed + "ms");
    }

    static Mono<String> profile() { return Mono.just("profile").delayElement(Duration.ofMillis(300)); }
    static Mono<String> posts()   { return Mono.just("posts").delayElement(Duration.ofMillis(500)); }
    static Mono<String> friends() { return Mono.just("friends").delayElement(Duration.ofMillis(200)); }

    // -------------------------------------------------------------------
    // Zadatak 5 - combineLatest. Emituje cim stigne novi BILO GDE,
    // koristeci najnovije sa druge strane. Ne emituje dok BAR JEDAN
    // element ne dodje sa SVAKE strane (zato u pocetku ima pauze).
    // -------------------------------------------------------------------
    static void zadatak5() {
        Flux<String> textInput = Flux.just("a", "ab", "abc")
                .delayElements(Duration.ofMillis(200));
        Flux<String> filter = Flux.just("ALL", "ACTIVE")
                .delayElements(Duration.ofMillis(350));

        Flux.combineLatest(textInput, filter,
                        (t, f) -> "search(text=" + t + ", filter=" + f + ")")
                .doOnNext(v -> log("combineLatest", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 6 - merge dva nezavisna toka.
    //
    // Ukupno se emituje 10 dogadjaja (5 + 5). Redosled je vremenski
    // determinisan (po stizanju), ali nije "interleaved" po pravilu -
    // dva izvora rade nezavisno.
    // -------------------------------------------------------------------
    static void zadatak6() {
        Flux<String> klikovi = Flux.interval(Duration.ofMillis(150))
                .take(5)
                .map(n -> "klik-" + n);
        Flux<String> tasteri = Flux.interval(Duration.ofMillis(220))
                .take(5)
                .map(n -> "taster-" + n);

        Flux.merge(klikovi, tasteri)
                .doOnNext(v -> log("event", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 7 - buffer.
    //
    // (a) buffer(N): tačno 4 paketa od po 5 stavki.
    // (b) buffer(Duration): varljiv broj po paketu (~4 stavke po 200ms
    //     ako je delay 50ms).
    // (c) bufferTimeout(N, Duration): emit ili pri popunjavanju ili
    //     po isteku vremena - ono što prvo stigne.
    // -------------------------------------------------------------------
    static void zadatak7() {
        System.out.println("(a) buffer(5):");
        Flux.range(1, 20)
                .delayElements(Duration.ofMillis(50))
                .buffer(5)
                .doOnNext(b -> log("buffer(5)", b))
                .blockLast();

        System.out.println("\n(b) buffer(Duration 200ms):");
        Flux.range(1, 20)
                .delayElements(Duration.ofMillis(50))
                .buffer(Duration.ofMillis(200))
                .doOnNext(b -> log("buffer(200ms)", b))
                .blockLast();

        System.out.println("\n(c) bufferTimeout(5, 200ms):");
        Flux.range(1, 20)
                .delayElements(Duration.ofMillis(50))
                .bufferTimeout(5, Duration.ofMillis(200))
                .doOnNext(b -> log("bufferTimeout", b))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 8 - groupBy.
    //
    // Posto su sve operacije sinhrone i bez subscribeOn, ceo pipeline
    // ide na main niti. Bez delay-a, redosled emisija je determinističan.
    // -------------------------------------------------------------------
    static void zadatak8() {
        Flux<String> rec = Flux.just(
                "ana", "ALEKSANDAR", "Marko", "milan", "PETAR", "pavle", "Jovana");

        rec
                .groupBy(r -> Character.toLowerCase(r.charAt(0)))
                .flatMap(group -> group
                        .map(String::toUpperCase)
                        .collectList()
                        .map(list -> "slovo " + group.key() + " -> " + list))
                .doOnNext(System.out::println)
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Zadatak 9 - expand za paginaciju.
    //
    // expand pokrece fetch(0), emituje stranicu, pa za nju pokrene
    // fetch(1) (ako imaSledecu), itd. Staje kad expander vrati
    // Mono.empty (kad poslednja stranica kaze imaSledecu=false).
    //
    // Sve stavke se kupe sa flatMapIterable + collectList.
    // -------------------------------------------------------------------
    static void zadatak9() {
        List<String> sveStavke = fetch(0)
                .expand(s -> s.imaSledecu()
                        ? fetch(s.broj() + 1)
                        : Mono.empty())
                .flatMapIterable(Stranica::stavke)
                .collectList()
                .block();
        System.out.println("Sve stavke: " + sveStavke);
    }

    record Stranica(int broj, List<String> stavke, boolean imaSledecu) {}

    static Mono<Stranica> fetch(int broj) {
        System.out.println("  fetch stranice " + broj);
        return Mono.just(new Stranica(
                        broj,
                        List.of("s" + broj + ".1", "s" + broj + ".2"),
                        broj < 3))
                .delayElement(Duration.ofMillis(100));
    }

    // -------------------------------------------------------------------
    // Zadatak 10 - flatMapIterable.
    //
    // Prazna lista nije problem - flatMapIterable je samo "preskoči"
    // (nema sta da emituje). Tip transformacije je T → Iterable<R>, sa
    // sinhronom evaluacijom.
    // -------------------------------------------------------------------
    static void zadatak10() {
        Flux.just(List.of(1, 2, 3), List.of(4), List.<Integer>of(), List.of(5, 6))
                .flatMapIterable(lista -> lista)
                .doOnNext(System.out::println)
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Pomocni log - vreme + nit + tag + vrednost.
    // -------------------------------------------------------------------
    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-14s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
