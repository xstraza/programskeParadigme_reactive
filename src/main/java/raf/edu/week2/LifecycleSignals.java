package raf.edu.week2;

import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Nedelja 2 — Lifecycle, signali i doOn* hooks.
 *
 * Svaki Subscriber prolazi kroz tačno ovaj redosled signala:
 *
 *     onSubscribe(Subscription)        ← uvek prvi
 *        ↓
 *     onNext(elem)*                    ← 0 ili više puta
 *        ↓
 *     onComplete()  XOR  onError(...)  ← terminalni
 *
 * Reactor nam daje DVA načina da se "zakačimo":
 *   1. subscribe(...) — TERMINALNI; aktivira tok (bez subscribe — ništa).
 *   2. doOn* hooks    — međupozicije; ne aktiviraju tok, samo posmatraju.
 *
 * Demo pokriva:
 *   - varijante subscribe (od fire-and-forget do full Subscriber-a)
 *   - svaki doOn* hook redom
 *   - log() — zlatni alat za debug
 */
public class LifecycleSignals {

    public static void main(String[] args) {
        System.out.println("=== 1. subscribe varijante ===\n");
        subscribeVarijante();

        System.out.println("\n=== 2. doOn* hooks po redu ===\n");
        doOnHooks();

        System.out.println("\n=== 3. doOnError vs doFinally vs doOnTerminate ===\n");
        terminalniHooks();

        System.out.println("\n=== 4. doOnCancel — kad se subscriber otkaže ===\n");
        cancelHook();

        System.out.println("\n=== 5. log() operator ===\n");
        logOperator();
    }

    // -------------------------------------------------------------------
    // subscribe ima vise overload-a. Kreni od najjednostavnijeg.
    // -------------------------------------------------------------------
    static void subscribeVarijante() {
        // (a) Bez ijednog handlera — fire-and-forget.
        Flux.just(1, 2, 3).subscribe();
        System.out.println("  [a] subscribe() — bez handlera, ništa se ne ispiše");

        // (b) Samo onNext.
        Flux.just(1, 2, 3).subscribe(
                v -> System.out.println("  [b] onNext: " + v));

        // (c) onNext + onError.
        Flux.<Integer>error(new RuntimeException("boom")).subscribe(
                v   -> System.out.println("  [c] onNext: " + v),
                err -> System.out.println("  [c] onError: " + err.getMessage()));

        // (d) onNext + onError + onComplete.
        Flux.just(10, 20).subscribe(
                v   -> System.out.println("  [d] onNext: " + v),
                err -> System.out.println("  [d] onError: " + err),
                ()  -> System.out.println("  [d] onComplete"));

        // (e) onNext + onError + onComplete + onSubscribe (kontrola request-a).
        Flux.range(1, 5).subscribe(
                v   -> System.out.println("  [e] onNext: " + v),
                err -> {},
                ()  -> System.out.println("  [e] onComplete"),
                sub -> {
                    System.out.println("  [e] onSubscribe — tražim 2");
                    sub.request(2);   // kontrolisani backpressure — uzimamo samo 2
                });
    }

    // -------------------------------------------------------------------
    // Svi doOn* hooks po redu, samo posmatraju signale.
    // -------------------------------------------------------------------
    static void doOnHooks() {
        Flux.range(1, 3)
                .doOnSubscribe(sub -> System.out.println("  [hook] doOnSubscribe"))
                .doOnRequest(n     -> System.out.println("  [hook] doOnRequest: " + n))
                .doOnNext(v        -> System.out.println("  [hook] doOnNext: " + v))
                .doOnComplete(()   -> System.out.println("  [hook] doOnComplete"))
                .doOnTerminate(()  -> System.out.println("  [hook] doOnTerminate (complete ili error)"))
                .doFinally(sig     -> System.out.println("  [hook] doFinally: " + sig))
                .subscribe();
    }

    // -------------------------------------------------------------------
    // Razlika: doOnError, doOnTerminate, doFinally.
    //
    // doOnError      — samo na onError
    // doOnTerminate  — onComplete ili onError (ali NE na cancel)
    // doFinally      — uvek (i na cancel) sa SignalType-om
    // -------------------------------------------------------------------
    static void terminalniHooks() {
        Flux.<Integer>error(new IllegalStateException("simulirana"))
                .doOnError(err     -> System.out.println("  [err] doOnError: " + err.getMessage()))
                .doOnTerminate(()  -> System.out.println("  [err] doOnTerminate"))
                .doFinally(sig     -> System.out.println("  [err] doFinally: " + sig))
                .subscribe(
                        v   -> {},
                        err -> {});   // konzumiramo error da ne ode na stderr
    }

    // -------------------------------------------------------------------
    // doOnCancel — kad subscriber otkaže pre kraja.
    // Pokrenemo beskonačan interval, uzmemo prvih 2, pa će se interval
    // CANCEL-ovati interno (jer take(2) otkaže izvor).
    // -------------------------------------------------------------------
    static void cancelHook() {
        Flux.interval(Duration.ofMillis(50))
                .doOnCancel(()   -> System.out.println("  [cancel] doOnCancel"))
                .doFinally(sig   -> System.out.println("  [cancel] doFinally: " + sig))
                .take(2)
                .doOnNext(v      -> System.out.println("  [cancel] onNext: " + v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // log() — automatski ispisuje SVE signale. Najbolji alat za debug.
    // Možemo ga staviti na više mesta u pipeline-u i videti šta gde
    // pristiže.
    // -------------------------------------------------------------------
    static void logOperator() {
        Flux.range(1, 3)
                .log("posle.range")
                .map(n -> n * 10)
                .log("posle.map")
                .filter(n -> n > 10)
                .log("posle.filter")
                .subscribe();
    }
}
