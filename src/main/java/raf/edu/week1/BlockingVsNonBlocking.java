package raf.edu.week1;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 1 - Blokirajući vs. neblokirajući model
 *
 * Konkretizacija "Era 2 vs. Era 4" iz README-a, sekcija 2:
 *  - Era 2 (thread-per-request): nit BLOKIRA dok čeka. 100 paralelnih
 *    "poziva" zahteva 100 niti, svaka troši ~1 MB stack-a.
 *  - Era 4 (reaktivno): nit ne blokira na čekanju. Mali pool niti
 *    (parallel scheduler ~ broj CPU-a) opslužuje 100 paralelnih
 *    "poziva" za isto vreme.
 *
 * Simulacija: 100 paralelnih operacija, svaka traje 200 ms.
 * Idealno (potpuno paralelno) vreme: 200 ms.
 *
 * Demonstriramo tri scenarija:
 *  1. blockingThreadPerRequest - 1 nit po pozivu (Era 2 maksimum).
 *  2. blockingFixedPool         - pool od 8 niti (Era 2 ekonomično).
 *  3. nonBlockingReactor        - Mono.delay (Era 4).
 *
 * Cilj: u konzoli jasno videti "peak thread count" i ukupno vreme,
 * pa izvući zaključak. Ovo nije teorija - to su brojevi.
 */
public class BlockingVsNonBlocking {

    private static final int N = 100;
    private static final Duration LATENCY = Duration.ofMillis(200);

    public static void main(String[] args) throws Exception {
        System.out.println("=== Scenario: " + N + " paralelnih 'poziva', svaki traje "
                + LATENCY.toMillis() + " ms ===\n");
        System.out.println("Idealno (potpuno paralelno) vreme: " + LATENCY.toMillis() + " ms\n");

        System.out.println("\n--- Verzija 1: thread-per-request (1 nit po pozivu) ---");
        blockingThreadPerRequest();

        System.out.println("\n--- Verzija 2: fixed pool od 8 niti ---");
        blockingFixedPool();

        System.out.println("\n--- Verzija 3: neblokirajući (Reactor parallel scheduler) ---");
        nonBlockingReactor();

        System.out.println("\n=== Zaključci ===");
        System.out.println("""
                  - Verzija 1 dostiže ~200ms ALI po cenu ~100 fizičkih niti.
                    Skalira do nekoliko hiljada - pa pada (OOM, context switch).
                  - Verzija 2 troši samo 8 niti, ali zato traje N/8 batch-eva.
                    Ekonomično, ali sporo na čekanju.
                  - Verzija 3 ima oba: malo niti I brzo. Trik nije magija
                    - Mono.delay NE drži nit, prijavi se timer-u i pusti.
                """);
    }

    // -----------------------------------------------------------------------
    // Verzija 1 - thread-per-request.
    // 100 niti istovremeno, svaka spava 200ms. Vreme: ~200ms.
    // Cena: ~100 paralelnih niti u VM-u (svaka ~1MB stack-a).
    // -----------------------------------------------------------------------
    static void blockingThreadPerRequest() throws InterruptedException {
        AtomicInteger peakThreads = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        // Sampler - tokom rada periodično meri broj aktivnih niti.
        Thread sampler = pokreniSamplerNiti(peakThreads);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            Thread t = new Thread(() -> {
                try { Thread.sleep(LATENCY); } catch (InterruptedException ignored) {}
            }, "demo-blocking-" + i);
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) t.join();

        sampler.interrupt();
        long duration = System.currentTimeMillis() - start;

        System.out.printf("  Ukupno vreme: %d ms%n", duration);
        System.out.printf("  Peak broj aktivnih niti tokom rada: %d%n", peakThreads.get());
    }

    // -----------------------------------------------------------------------
    // Verzija 2 - fiksni pool od 8 niti.
    // 100 poziva mora kroz 8 niti - N/8 = ~13 batch-eva po 200ms.
    // Vreme: ~13 × 200 = ~2600ms. Niti: 8.
    // -----------------------------------------------------------------------
    static void blockingFixedPool() throws Exception {
        int poolSize = 8;
        AtomicInteger peakThreads = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        Thread sampler = pokreniSamplerNiti(peakThreads);

        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try { Thread.sleep(LATENCY); } catch (InterruptedException ignored) {}
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        executor.shutdown();

        sampler.interrupt();
        long duration = System.currentTimeMillis() - start;

        long ocekivano = (long) Math.ceil((double) N / poolSize) * LATENCY.toMillis();
        System.out.printf("  Pool veličina: %d niti%n", poolSize);
        System.out.printf("  Ukupno vreme: %d ms (očekivano ~%d ms = ceil(%d/%d) × %d)%n",
                duration, ocekivano, N, poolSize, LATENCY.toMillis());
        System.out.printf("  Peak broj aktivnih niti tokom rada: %d%n", peakThreads.get());
    }

    // -----------------------------------------------------------------------
    // Verzija 3 - Reactor Mono.delay.
    // delay NE BLOKIRA nit - prijavi se internom timer-u i nit oslobodi.
    // Sa parallel scheduler-om (~ broj CPU-a niti), 100 paralelnih
    // "poziva" završi za ~200ms. Niti: ~ broj CPU-a.
    // -----------------------------------------------------------------------
    static void nonBlockingReactor() {
        AtomicInteger peakThreads = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        Thread sampler = pokreniSamplerNiti(peakThreads);

        Long uradjeno = Flux.range(1, N)
                // flatMap pokrene SVE Mono.delay paralelno (do unutrašnjeg
                // concurrency limita, default 256).
                .flatMap(i -> Mono.delay(LATENCY).thenReturn(i))
                .count()
                .block();   // block samo zato što smo u main-u

        sampler.interrupt();
        long duration = System.currentTimeMillis() - start;

        System.out.printf("  Ukupno vreme: %d ms%n", duration);
        System.out.printf("  Procesirano 'poziva': %d%n", uradjeno);
        System.out.printf("  Peak broj aktivnih niti tokom rada: %d%n", peakThreads.get());
        System.out.println("  (Reactor parallel scheduler ima ~ broj CPU-a niti - vidi peak)");
    }

    // -----------------------------------------------------------------------
    // Pomoćna metoda - startuje sampler nit koja na svakih 5ms zabeleži
    // trenutni Thread.activeCount() i ažurira peak. Nije precizan
    // benchmark, ali za demo dovoljno.
    // -----------------------------------------------------------------------
    static Thread pokreniSamplerNiti(AtomicInteger peakThreads) {
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                int trenutno = Thread.activeCount();
                peakThreads.updateAndGet(p -> Math.max(p, trenutno));
                try { Thread.sleep(5); } catch (InterruptedException e) { return; }
            }
        }, "sampler");
        sampler.setDaemon(true);
        sampler.start();
        return sampler;
    }
}
