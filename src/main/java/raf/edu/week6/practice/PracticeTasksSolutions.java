package raf.edu.week6.practice;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * Nedelja 6 - rešenja praktičnih zadataka.
 */
public class PracticeTasksSolutions {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static final HttpClient CLIENT = HttpClient.create();

    public static void main(String[] args) {
        // Otkomentariši zadatak na kome radiš.

        // zadatak1();
        // zadatak2();
        // zadatak3();
        // zadatak4();
        // zadatak5();
        // zadatak6();
        // zadatak7();
        // zadatak8();
        // zadatak9();
        // zadatak10();
    }

    // ===================================================================
    // Zadatak 1 - HTTP GET sa fallback-om
    // ===================================================================
    static void zadatak1() {
        String rez = CLIENT.get()
                .uri("https://jsonplaceholder.typicode.com/posts/2")
                .responseContent()
                .aggregate()
                .asString()
                .timeout(Duration.ofSeconds(2))
                .onErrorReturn("FALLBACK")
                .block();

        log("odgovor", skrati(rez));
    }

    // ===================================================================
    // Zadatak 2 - paralelni HTTP GET-ovi
    // ===================================================================
    static void zadatak2() {
        long t0 = System.currentTimeMillis();

        List<String> rezultati = Flux.range(1, 5)
                .flatMap(id -> CLIENT.get()
                                .uri("https://jsonplaceholder.typicode.com/posts/" + id)
                                .responseContent()
                                .aggregate()
                                .asString()
                                .map(b -> "post#" + id),
                        5)
                .collectList()
                .block();

        log("vreme", (System.currentTimeMillis() - t0) + "ms");
        log("rezultati", rezultati);
    }

    // ===================================================================
    // Zadatak 3 - multicast event bus
    // ===================================================================
    static void zadatak3() {
        Sinks.Many<String> bus = Sinks.many().multicast().onBackpressureBuffer();

        bus.asFlux().subscribe(v -> log("sub1", v));
        bus.asFlux().subscribe(v -> log("sub2", v));

        bus.emitNext("a", FAIL_FAST);
        bus.emitNext("b", FAIL_FAST);
        bus.emitNext("c", FAIL_FAST);
        bus.emitComplete(FAIL_FAST);
    }

    // ===================================================================
    // Zadatak 4 - replay sa kasnim subscriber-om
    // ===================================================================
    static void zadatak4() {
        Sinks.Many<Integer> bus = Sinks.many().replay().limit(5);

        for (int i = 1; i <= 10; i++) {
            bus.emitNext(i, FAIL_FAST);
        }

        log("info", "Sad se pretplaćujem - očekujem 6,7,8,9,10:");
        bus.asFlux().subscribe(v -> log("kasni-sub", v));

        bus.emitComplete(FAIL_FAST);
    }

    // ===================================================================
    // Zadatak 5 - brojač pomoću scan-a
    // ===================================================================
    static void zadatak5() {
        Sinks.Many<Integer> akcije = Sinks.many().multicast().onBackpressureBuffer();

        akcije.asFlux()
                .scan(0, Integer::sum)
                .skip(1)                              // preskoči seed
                .subscribe(stanje -> log("brojač", stanje));

        akcije.emitNext(+1, FAIL_FAST);
        akcije.emitNext(+1, FAIL_FAST);
        akcije.emitNext(+1, FAIL_FAST);
        akcije.emitNext(-1, FAIL_FAST);
        akcije.emitNext(+1, FAIL_FAST);
        akcije.emitComplete(FAIL_FAST);
    }

    // ===================================================================
    // Zadatak 6 - Redux-stil TODO lista
    // ===================================================================
    sealed interface TodoAction permits Add, Toggle, Remove {}
    record Add(String text) implements TodoAction {}
    record Toggle(int id) implements TodoAction {}
    record Remove(int id) implements TodoAction {}

    record TodoItem(int id, String text, boolean done) {}

    static void zadatak6() {
        Sinks.Many<TodoAction> akcije = Sinks.many().multicast().onBackpressureBuffer();

        akcije.asFlux()
                .scan(List.<TodoItem>of(), (state, action) -> switch (action) {
                    case Add a -> {
                        List<TodoItem> n = new ArrayList<>(state);
                        n.add(new TodoItem(state.size() + 1, a.text(), false));
                        yield List.copyOf(n);
                    }
                    case Toggle t -> state.stream()
                            .map(it -> it.id() == t.id()
                                    ? new TodoItem(it.id(), it.text(), !it.done())
                                    : it)
                            .toList();
                    case Remove r -> state.stream()
                            .filter(it -> it.id() != r.id())
                            .toList();
                })
                .distinctUntilChanged()
                .subscribe(s -> log("ui", s));

        akcije.emitNext(new Add("kupi mleko"),  FAIL_FAST);
        akcije.emitNext(new Add("javi mami"),   FAIL_FAST);
        akcije.emitNext(new Add("plati račun"), FAIL_FAST);
        akcije.emitNext(new Toggle(2),          FAIL_FAST);
        akcije.emitNext(new Remove(1),          FAIL_FAST);
        akcije.emitComplete(FAIL_FAST);
    }

    // ===================================================================
    // Zadatak 7 - ETL sa batching-om
    // ===================================================================
    static void zadatak7() {
        Flux.range(1, 100)
                .flatMap(n -> Mono.fromCallable(() -> {
                                    sleep(20);
                                    return n * n;
                                })
                                .subscribeOn(Schedulers.boundedElastic()),
                        8)
                .buffer(10)
                .doOnNext(batch -> log("batch-upis", batch))
                .blockLast();
    }

    // ===================================================================
    // Zadatak 8 - error isolation u ETL-u
    // ===================================================================
    static void zadatak8() {
        AtomicInteger lose = new AtomicInteger();

        List<Integer> rezultati = Flux.range(1, 20)
                .flatMap(n -> Mono.fromCallable(() -> parseRow(n))
                                .subscribeOn(Schedulers.boundedElastic())
                                .onErrorResume(ex -> {
                                    lose.incrementAndGet();
                                    log("preskoči", n + ": " + ex.getMessage());
                                    return Mono.empty();
                                }),
                        4)
                .collectList()
                .block();

        log("uspesni", rezultati);
        log("statistika", "odbačeno=" + lose.get());
    }

    static int parseRow(int n) {
        if (n % 2 == 0) throw new RuntimeException("paran " + n);
        return n * 10;
    }

    // ===================================================================
    // Zadatak 9 - paralelni dashboard sa zip-om
    // ===================================================================
    static void zadatak9() {
        long t0 = System.currentTimeMillis();

        Mono<String> svcA = Mono.delay(Duration.ofMillis(100)).map(t -> "A");
        Mono<String> svcB = Mono.delay(Duration.ofMillis(150)).map(t -> "B");
        Mono<String> svcC = Mono.delay(Duration.ofMillis(200)).map(t -> "C");

        String rez = Mono.zip(svcA, svcB, svcC)
                .map(t -> t.getT1() + "|" + t.getT2() + "|" + t.getT3())
                .block();

        log("rezultat", rez + " (" + (System.currentTimeMillis() - t0) + "ms)");
    }

    // ===================================================================
    // Zadatak 10 - resilient dashboard
    // ===================================================================
    static void zadatak10() {
        long t0 = System.currentTimeMillis();

        Retry policy = Retry.backoff(2, Duration.ofMillis(50))
                .jitter(0.5)
                .filter(ex -> ex instanceof IOException
                           || ex instanceof TimeoutException);

        Mono<String> svcA = Mono.fromCallable(() -> { sleep(50); return "A"; })
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> svcB = Mono.<String>fromCallable(() -> {
                    sleep(50);
                    throw new IOException("svcB always fails");
                })
                .subscribeOn(Schedulers.boundedElastic());
        Mono<String> svcC = Mono.fromCallable(() -> { sleep(800); return "C"; })
                .subscribeOn(Schedulers.boundedElastic());

        String rez = Mono.zip(
                        svcA.timeout(Duration.ofMillis(300)).retryWhen(policy)
                                .onErrorReturn("DEF-A"),
                        svcB.timeout(Duration.ofMillis(300)).retryWhen(policy)
                                .onErrorReturn("DEF-B"),
                        svcC.timeout(Duration.ofMillis(300)).retryWhen(policy)
                                .onErrorReturn("DEF-C"))
                .map(t -> "A=" + t.getT1() + " B=" + t.getT2() + " C=" + t.getT3())
                .timeout(Duration.ofSeconds(1))
                .block();

        log("rezultat", rez + " (" + (System.currentTimeMillis() - t0) + "ms)");
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    static String skrati(String s) {
        if (s == null) return "null";
        String oneline = s.replace('\n', ' ').replaceAll("\\s+", " ");
        return oneline.length() > 120 ? oneline.substring(0, 120) + "..." : oneline;
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-13s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
