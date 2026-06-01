package raf.edu.week6.caching;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 6 - caching / memoizacija reaktivnog izvora.
 * <p>
 * Mono/Flux su po default-u "cold" - svaki subscribe POKREĆE izvor
 * iznova. Za skup poziv (HTTP, DB, teško računanje) to znači da N
 * subscriber-a = N poziva. cache() pretvara izvor u "hot": prvi
 * subscribe ga pokrene, rezultat se zapamti, svi sledeći dobiju
 * zapamćenu vrednost bez ponovnog izvršavanja.
 * <p>
 * Tri varijante:
 * 1. cache()            - zapamti zauvek (dok JVM živi).
 * 2. cache(Duration)    - TTL; posle isteka prvi sledeći subscribe
 *                         ponovo pokrene izvor (klasičan cache-with-ttl).
 * 3. cache() kao dedup  - više paralelnih poziva dele JEDAN in-flight
 *                         izvor (sprečava "cache stampede").
 * <p>
 * Most ka nedelji 7: cache() je jedan od operatora za hot/cold
 * konverziju (uz share(), replay(), publish()).
 */
public class CachingDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // Brojač koliko je puta "skupi" izvor stvarno izvršen. Ako caching
    // radi, ovaj broj NE raste sa svakim subscribe-om.
    private static final AtomicInteger brojPoziva = new AtomicInteger();

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. COLD - bez cache-a svaki subscribe ponovo zove izvor ===\n");
        primer1_coldBezCachea();

        System.out.println("\n=== 2. cache() - izvor se izvrši TAČNO jednom ===\n");
        primer2_cache();

        System.out.println("\n=== 3. cache(ttl) - zapamti na 1s, pa osveži ===\n");
        primer3_cacheSaTtl();

        System.out.println("\n=== 4. cache() kao dedup paralelnih poziva (anti-stampede) ===\n");
        primer4_dedupInFlight();
    }

    // -------------------------------------------------------------------
    // "Skup" izvor: simulira HTTP/DB poziv koji traje 200ms. Svaki put
    // kad se stvarno izvrši, uveća brojPoziva i to ispiše.
    // -------------------------------------------------------------------
    static Mono<String> skupiPoziv() {
        return Mono.fromCallable(() -> {
                    int n = brojPoziva.incrementAndGet();
                    log("IZVOR", "stvarno izvršavanje #" + n);
                    return "rezultat-" + n;
                })
                .delayElement(Duration.ofMillis(200));   // simulira latenciju mreže
    }

    // -------------------------------------------------------------------
    // Bez cache-a: dva subscribe-a = dva izvršavanja izvora. brojPoziva
    // poraste na 2. Tipičan bug - mislimo da smo pozvali servis jednom,
    // a svaki .subscribe()/.block() ga ponovo pokrene.
    // -------------------------------------------------------------------
    static void primer1_coldBezCachea() {
        brojPoziva.set(0);
        Mono<String> izvor = skupiPoziv();              // bez cache-a

        log("sub-1", izvor.block());
        log("sub-2", izvor.block());                    // PONOVO zove izvor
        log("info", "ukupno izvršavanja: " + brojPoziva.get() + " (očekivano 2)");
    }

    // -------------------------------------------------------------------
    // .cache(): prvi subscribe pokrene izvor, rezultat se zapamti.
    // Drugi subscribe NE pokreće izvor - vraća zapamćeno odmah.
    // brojPoziva ostaje 1.
    // -------------------------------------------------------------------
    static void primer2_cache() {
        brojPoziva.set(0);
        Mono<String> kesirano = skupiPoziv().cache();   // zapamti zauvek

        log("sub-1", kesirano.block());
        log("sub-2", kesirano.block());                 // iz keša, bez poziva
        log("sub-3", kesirano.block());
        log("info", "ukupno izvršavanja: " + brojPoziva.get() + " (očekivano 1)");
    }

    // -------------------------------------------------------------------
    // .cache(Duration): vrednost važi 1s. Posle isteka, sledeći
    // subscribe ponovo pokrene izvor (i opet keširaj na 1s).
    //
    // Tipično za "konfiguracija/feature-flags koji se retko menjaju" -
    // ne gađaj servis svaki put, ali ni ne drži vrednost zauvek.
    // -------------------------------------------------------------------
    static void primer3_cacheSaTtl() throws InterruptedException {
        brojPoziva.set(0);
        Mono<String> kesirano = skupiPoziv().cache(Duration.ofSeconds(1));

        log("t=0ms", kesirano.block());                 // poziv #1
        log("t~0ms", kesirano.block());                 // iz keša
        Thread.sleep(1200);                             // pusti da TTL istekne
        log("t=1.2s", kesirano.block());                // poziv #2 (osvežen)
        log("info", "ukupno izvršavanja: " + brojPoziva.get() + " (očekivano 2)");
    }

    // -------------------------------------------------------------------
    // Anti-stampede: 5 poziva KRENE skoro istovremeno dok izvor još
    // traje (200ms). Sa cache(), svi dele JEDAN in-flight izvor - kad
    // stigne, svi dobiju isti rezultat. Bez cache-a, dobili bismo 5
    // paralelnih poziva ka servisu ("cache stampede").
    // -------------------------------------------------------------------
    static void primer4_dedupInFlight() throws InterruptedException {
        brojPoziva.set(0);
        Mono<String> deljeno = skupiPoziv().cache();

        for (int i = 1; i <= 5; i++) {
            int id = i;
            deljeno.subscribe(v -> log("klijent-" + id, v));
        }

        Thread.sleep(400);                              // sačekaj da izvor stigne
        log("info", "ukupno izvršavanja: " + brojPoziva.get() + " (očekivano 1, ne 5)");
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
