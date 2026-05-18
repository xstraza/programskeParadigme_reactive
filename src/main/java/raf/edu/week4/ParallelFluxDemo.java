package raf.edu.week4;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 4 - eksplicitni paralelizam preko Flux.parallel().
 *
 * Za razliku od flatMap-a (koji daje paralelizam za ASYNC unutrašnje
 * tokove), parallel() služi da SINHRONI rad razbije na više CPU jezgara.
 *
 * Tri obavezna koraka:
 *
 *   Flux<T>          source
 *       .parallel(N)            ──► ParallelFlux<T>  (deli na N "rail"-ova)
 *       .runOn(scheduler)       ──► svaki rail dobija worker scheduler-a
 *       .map / .filter / ...    ──► operatori trče paralelno po rail-ovima
 *       .sequential()           ──► vraćamo se u Flux<T>
 *
 * Bez .runOn(...) parallel() ne radi ništa korisno - operatori još uvek
 * idu na nit subscription-a. .runOn je ono što stvarno paralelizuje.
 */
public class ParallelFluxDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. Osnovno - parallel + runOn + sequential ===\n");
        primer1_osnovno();

        System.out.println("\n=== 2. Bez .runOn - parallel() je samo 'podela' bez efekta ===\n");
        primer2_bezRunOn();

        System.out.println("\n=== 3. CPU heavy posao - koliko je brže? ===\n");
        primer3_cpuHeavy();

        System.out.println("\n=== 4. parallel().runOn(io) za blocking - anti-pattern ===\n");
        primer4_blockingAntiPattern();
    }

    // -------------------------------------------------------------------
    // Osnovni primer - 8 elemenata, 4 rail-a na parallel scheduler-u.
    // Vidimo da se svaki rail izvšava na parallel-1, parallel-2, parallel-3,
    // parallel-4 - round-robin.
    // -------------------------------------------------------------------
    static void primer1_osnovno() {
        Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(n -> {
                    log("compute", n);
                    return n * n;
                })
                .sequential()
                .doOnNext(r -> log("result", r))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // BEZ .runOn - parallel() podeli na rail-ove ali svi rade na istoj
    // niti (na main, ovde). Tj. nikakvog paralelizma nema.
    //
    // Zaključak: .runOn(scheduler) je obavezan deo recepta.
    // -------------------------------------------------------------------
    static void primer2_bezRunOn() {
        Flux.range(1, 6)
                .parallel(3)
                // .runOn(...) NAMERNO IZOSTAVLJEN
                .map(n -> {
                    log("no-runOn", n);
                    return n * 10;
                })
                .sequential()
                .blockLast();
    }

    // -------------------------------------------------------------------
    // CPU heavy - sinhroni posao koji traje ~100ms (simulacija).
    //
    // 8 elemenata × 100ms = 800ms sekvencijalno.
    // Sa parallel(4) na 4 jezgra: ~200ms (4× brže).
    // -------------------------------------------------------------------
    static void primer3_cpuHeavy() {
        log("info", "sekvencijalno:");
        long t0 = System.currentTimeMillis();
        Flux.range(1, 8)
                .map(ParallelFluxDemo::cpuHeavyTask)
                .blockLast();
        log("info", "sekvencijalno trajalo: " + (System.currentTimeMillis() - t0) + "ms");

        log("info", "paralelno:");
        long t1 = System.currentTimeMillis();
        Flux.range(1, 8)
                .parallel(4)
                .runOn(Schedulers.parallel())
                .map(ParallelFluxDemo::cpuHeavyTask)
                .sequential()
                .blockLast();
        log("info", "paralelno trajalo:    " + (System.currentTimeMillis() - t1) + "ms");
    }

    // CPU heavy simulacija - busy loop koji ne spava (sleep bi pustio nit).
    static int cpuHeavyTask(int n) {
        long end = System.nanoTime() + Duration.ofMillis(100).toNanos();
        int x = n;
        while (System.nanoTime() < end) {
            x = (x * 1103515245 + 12345) & 0x7FFFFFFF;    // pseudo-random posao
        }
        return x;
    }

    // -------------------------------------------------------------------
    // ANTI-PATTERN: parallel().runOn(boundedElastic) sa blocking pozivima.
    //
    // Deluje da radi - elastic pool ima dosta niti pa će izdržati.
    // Ali kombinacija je nečitljiva i lako preraste ograničenja
    // pool-a. Za blocking I/O preporuka je:
    //
    //   flux.flatMap(x -> Mono.fromCallable(...).subscribeOn(boundedElastic))
    //
    // - to je čistiji pattern i daje istu kontrolu paralelizma kroz
    // concurrency parametar flatMap-a.
    // -------------------------------------------------------------------
    static void primer4_blockingAntiPattern() {
        log("info", "ovo radi ali nije pravi pattern za blocking:");

        Flux.range(1, 4)
                .parallel(4)
                .runOn(Schedulers.boundedElastic())
                .map(n -> {
                    log("blocking", n);
                    try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                    return n;
                })
                .sequential()
                .blockLast();

        log("info", "preporučeni pattern za blocking:");

        Flux.range(1, 4)
                .flatMap(n -> reactor.core.publisher.Mono.fromCallable(() -> {
                                    log("flatMap", n);
                                    Thread.sleep(100);
                                    return n;
                                })
                                .subscribeOn(Schedulers.boundedElastic()))
                .blockLast();
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-18s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
