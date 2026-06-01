package raf.edu.week6.pipeline;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nedelja 6 - ETL pipeline.
 *
 * ETL = Extract, Transform, Load. Klasičan batch obrazac. Sa
 * reaktivnim API-jem dobijamo:
 *   - paralelnu obradu (flatMap sa concurrency-jem),
 *   - batching pre upisa (buffer / windowTimeout),
 *   - error isolation (loš red ne ruši ceo posao),
 *   - backpressure (čitamo onoliko brzo koliko consumer može).
 *
 * Domen primera: parsiranje CSV linija "id,price,country" -> obogaćivanje
 * VAT-om po državi -> bulk upis u "DB".
 */
public class EtlPipelineDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // -------------------------------------------------------------------
    // Domen modeli
    // -------------------------------------------------------------------
    record SiroviRed(String csv) {}
    record Order(int id, double price, String country) {}
    record OrderSaPDV(int id, double price, double pdv, String country) {}

    public static void main(String[] args) {
        System.out.println("=== 1. Sekvencijalni ETL - bez paralelizacije ===\n");
        primer1_sekvencijalni();

        System.out.println("\n=== 2. Paralelni ETL - flatMap concurrency=4 ===\n");
        primer2_paralelni();

        System.out.println("\n=== 3. Batch upis - buffer(N) ===\n");
        primer3_batch();

        System.out.println("\n=== 4. windowTimeout - batch po vremenu I po broju ===\n");
        primer4_windowTimeout();

        System.out.println("\n=== 5. Error isolation - loš red preskačemo ===\n");
        primer5_errorIsolation();
    }

    // -------------------------------------------------------------------
    // Verzija bez paralelizacije - za poređenje. Svaki red se parsira
    // i obogaćuje pre nego što sledeći krene.
    //
    // 10 redova × (parse 20ms + enrich 50ms) ≈ 700ms.
    // -------------------------------------------------------------------
    static void primer1_sekvencijalni() {
        long t0 = System.currentTimeMillis();

        Flux<SiroviRed> izvor = ulazniRedovi(10);

        izvor
                .map(EtlPipelineDemo::parsiraj)             // sinhrono, ne paralelno
                .map(EtlPipelineDemo::obogati)              // isto
                .doOnNext(o -> log("loaded", o))
                .blockLast();

        log("vreme", "sekvencijalno: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // Paralelni ETL: parse i enrich su sad Mono-i koji idu na
    // boundedElastic scheduler (jer parse/enrich simuliraju blocking
    // I/O).
    //
    // flatMap(fn, concurrency=4) drži najviše 4 istovremenih unutrašnjih
    // Mono-a po koraku. Ukupno vreme ≈ 10/4 × 70ms = ~200ms.
    //
    // VAŽNO: redosled elemenata na izlazu flatMap-a NIJE garantovan
    // (zavisi ko prvi završi). Ako je redosled BITAN, koristi
    // concatMap (sporije) ili flatMapSequential (paralelno, ali izlaz
    // sortiran po ulaznom redosledu).
    // -------------------------------------------------------------------
    static void primer2_paralelni() {
        long t0 = System.currentTimeMillis();

        Flux<SiroviRed> izvor = ulazniRedovi(10);

        izvor
                .flatMap(red -> Mono.fromCallable(() -> parsiraj(red))
                                .subscribeOn(Schedulers.boundedElastic()),
                        /*concurrency=*/ 4)
                .flatMap(order -> Mono.fromCallable(() -> obogati(order))
                                .subscribeOn(Schedulers.boundedElastic()),
                        4)
                .doOnNext(o -> log("loaded", o))
                .blockLast();

        log("vreme", "paralelno: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // Batch obrada: umesto da pišemo red po red, sakupimo po 5 i radimo
    // "bulk insert".
    //
    // buffer(5) emituje List<T> svakih 5 elemenata. Kraj toka emituje
    // i poslednji (delimični) batch.
    //
    // U produkciji ovo CESTO znači razliku između 10000 DB poziva i
    // 200 - kašnjenje drastično pada.
    // -------------------------------------------------------------------
    static void primer3_batch() {
        long t0 = System.currentTimeMillis();

        ulazniRedovi(13)
                .flatMap(red -> Mono.fromCallable(() -> parsiraj(red))
                                .subscribeOn(Schedulers.boundedElastic()),
                        4)
                .map(EtlPipelineDemo::obogati)
                .buffer(5)                                  // List od 5 (poslednji možda manji)
                .flatMap(EtlPipelineDemo::bulkUpis)
                .blockLast();

        log("vreme", "batch: " + (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // windowTimeout(maxN, duration): zatvori batch kad ili stigne N
    // elemenata ILI prođe duration. Tipično za real-time pipeline-e
    // gde ne smemo da držimo elemente predugo.
    //
    // U primeru: maxN=10, duration=300ms. Pošto naš source emituje
    // sporo (svaki red 50ms), 300ms staje 5-6 redova - vidimo prevremene
    // batch-eve umesto da čekamo 10.
    // -------------------------------------------------------------------
    static void primer4_windowTimeout() {
        ulazniRedovi(8)
                .map(EtlPipelineDemo::parsiraj)
                .map(EtlPipelineDemo::obogati)
                .windowTimeout(10, Duration.ofMillis(300))
                .flatMap(Flux::collectList)
                .flatMap(EtlPipelineDemo::bulkUpis)
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Error isolation: loš red NE SME da pokvari ostatak fajla.
    //
    // Šablon: parse u flatMap-u, a tamo .onErrorResume(ex -> Mono.empty()).
    // Mono.empty() znači "preskoči ovaj element bez signala greške".
    //
    // Brojimo i loše i dobre redove na kraju - tipično za izveštaj.
    // -------------------------------------------------------------------
    static void primer5_errorIsolation() {
        AtomicInteger ok    = new AtomicInteger();
        AtomicInteger lose  = new AtomicInteger();

        Flux.just(
                        new SiroviRed("1,99.0,RS"),
                        new SiroviRed("2,150.0,DE"),
                        new SiroviRed("OVO_NIJE_BROJ,abc,XX"),     // loš
                        new SiroviRed("4,50.0,RS"),
                        new SiroviRed(""),                          // takođe loš
                        new SiroviRed("6,200.0,US"))
                .flatMap(red -> Mono.fromCallable(() -> parsiraj(red))
                        .onErrorResume(ex -> {
                            lose.incrementAndGet();
                            log("preskoči", "red='" + red.csv() + "' razlog=" + ex.getMessage());
                            return Mono.empty();
                        }))
                .map(EtlPipelineDemo::obogati)
                .doOnNext(o -> {
                    ok.incrementAndGet();
                    log("loaded", o);
                })
                .blockLast();

        log("izveštaj", "uspešno=" + ok.get() + ", odbačeno=" + lose.get());
    }

    // -------------------------------------------------------------------
    // E: izvor sirovih linija. Simuliramo "čitanje fajla" - svaki red
    // stiže sa malim kašnjenjem da imitira disk I/O.
    // -------------------------------------------------------------------
    static Flux<SiroviRed> ulazniRedovi(int koliko) {
        return Flux.range(1, koliko)
                .map(i -> new SiroviRed(i + "," + (i * 10.0) + ",RS"))
                .delayElements(Duration.ofMillis(20));
    }

    // -------------------------------------------------------------------
    // T1: parse CSV linije -> Order. Baca exception ako format nije ok.
    // Simulira blocking parse (npr. validacija, lookup).
    // -------------------------------------------------------------------
    static Order parsiraj(SiroviRed red) {
        sleep(20);
        String[] delovi = red.csv().split(",");
        if (delovi.length != 3) {
            throw new IllegalArgumentException("loš CSV format");
        }
        return new Order(
                Integer.parseInt(delovi[0]),
                Double.parseDouble(delovi[1]),
                delovi[2]);
    }

    // -------------------------------------------------------------------
    // T2: obogati Order PDV-om - simulira lookup u eksternom servisu
    // (50ms). Stope poreza po državi su sad hardkodirane radi demoa.
    // -------------------------------------------------------------------
    static OrderSaPDV obogati(Order o) {
        sleep(50);
        double stopa = switch (o.country()) {
            case "RS" -> 0.20;
            case "DE" -> 0.19;
            case "US" -> 0.00;
            default   -> 0.10;
        };
        return new OrderSaPDV(o.id(), o.price(), o.price() * stopa, o.country());
    }

    // -------------------------------------------------------------------
    // L: bulk upis (simuliran). Jedan poziv obrađuje ceo batch -
    // u stvarnom svetu ovde bi išao "INSERT INTO ... VALUES (...), (...), ...".
    // -------------------------------------------------------------------
    static Mono<Integer> bulkUpis(List<OrderSaPDV> batch) {
        return Mono.fromCallable(() -> {
                    sleep(30);   // jedan poziv, ne 5
                    log("bulkUpis", "size=" + batch.size() + " " + batch);
                    return batch.size();
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
