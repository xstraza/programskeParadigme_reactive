package raf.edu.week6.combining;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Nedelja 6 - kombinovanje izvora.
 *
 * Tipična "dashboard" agregacija - jedan endpoint vraća rezultat
 * spojen iz N pozadinskih servisa. Reaktivni model je naročito jak
 * ovde: paralelno čekanje, pojedinačni timeout/fallback, overall
 * timeout, retry samo gde ima smisla.
 *
 * Servisi koje simuliramo:
 *   - profilSvc      ~50ms,  retko pada
 *   - narudžbineSvc  ~120ms, ponekad pada (IOException)
 *   - preporukeSvc   ~200ms, često spor / pada
 *
 * Cilj: ukupno < 1s, sa što razumnijim degradacijama.
 */
public class CombiningDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // -------------------------------------------------------------------
    // Domen modeli - lagani records-i.
    // -------------------------------------------------------------------
    record Profil(int userId, String ime) {
        static Profil UNKNOWN = new Profil(-1, "<nepoznat>");
    }

    record Dashboard(Profil profil, List<String> narudžbine, List<String> preporuke) {}

    public static void main(String[] args) {
        System.out.println("=== 1. Mono.zip - paralelno čekanje na sve servise ===\n");
        primer1_zip();

        System.out.println("\n=== 2. Naivni zip - bez timeout-a, jedan pad ruši ceo zip ===\n");
        primer2_zipBezZastite();

        System.out.println("\n=== 3. Per-servis timeout + fallback - tihi degrade ===\n");
        primer3_perServisFallback();

        System.out.println("\n=== 4. Overall timeout - hard gornja granica ===\n");
        primer4_overallTimeout();

        System.out.println("\n=== 5. firstWithValue - prvi koji odgovori pobeđuje ===\n");
        primer5_firstWithValue();

        System.out.println("\n=== 6. Kompletan resilient dashboard ===\n");
        primer6_resilientDashboard();
    }

    // -------------------------------------------------------------------
    // Osnovni zip: sva 3 servisa krenu odjednom, čekamo da svi završe,
    // pa mapiramo Tuple3 u Dashboard.
    //
    // Vreme ≈ max(50, 120, 200) = 200ms, ne 370ms.
    // -------------------------------------------------------------------
    static void primer1_zip() {
        long t0 = System.currentTimeMillis();

        Dashboard d = Mono.zip(
                        profilSvc(1, false),
                        narudžbineSvc(1, false),
                        preporukeSvc(false))
                .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
                .block();

        log("dashboard", d);
        log("vreme", (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // Bez zaštite - ako narudžbineSvc baci IOException, ceo zip pada
    // i mi nemamo NIŠTA, čak iako su profil i preporuke uspeli.
    //
    // Ovo je default ponašanje koje retko želimo u realnom dashboard-u.
    // -------------------------------------------------------------------
    static void primer2_zipBezZastite() {
        try {
            Mono.zip(
                            profilSvc(1, false),
                            narudžbineSvc(1, /*pada=*/ true),
                            preporukeSvc(false))
                    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
                    .block();
        } catch (Exception ex) {
            log("greška", "zip pao: " + ex.getMessage()
                    + " (čak iako su profil i preporuke uspeli)");
        }
    }

    // -------------------------------------------------------------------
    // Per-servis fallback. Svaki Mono ima svoj timeout i svoj
    // onErrorReturn. Zip uvek uspeva - samo neki delovi mogu biti
    // "default" verzije.
    //
    // Pravilo izbora:
    //   - Profil je kritičan (Profil.UNKNOWN je marker da nešto ne radi)
    //   - Narudžbine fallback na prazan list (UI prikaže "nema narudžbi")
    //   - Preporuke su opcione (prazan list je sasvim ok)
    // -------------------------------------------------------------------
    static void primer3_perServisFallback() {
        long t0 = System.currentTimeMillis();

        Dashboard d = Mono.zip(
                        profilSvc(1, false)
                                .timeout(Duration.ofMillis(300))
                                .onErrorReturn(Profil.UNKNOWN),
                        narudžbineSvc(1, /*pada=*/ true)        // ovaj će pasti
                                .timeout(Duration.ofMillis(300))
                                .onErrorReturn(List.of()),
                        preporukeSvc(false)
                                .timeout(Duration.ofMillis(150))
                                .onErrorReturn(List.of()))
                .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
                .block();

        log("dashboard", d);
        log("vreme", (System.currentTimeMillis() - t0) + "ms");
    }

    // -------------------------------------------------------------------
    // Overall timeout: tvrda gornja granica. Korisno za SLA - npr.
    // "dashboard mora da odgovori u 1s, šta god da se dešava ispod".
    //
    // Kombinujemo sa per-servis fallback-om: per-servis timeout daje
    // šansu da pojedinačni servis bude spor i da ga zamenimo
    // default-om; overall daje gvozdenu gornju granicu.
    // -------------------------------------------------------------------
    static void primer4_overallTimeout() {
        long t0 = System.currentTimeMillis();

        try {
            Mono.zip(
                            profilSvc(1, false),
                            narudžbineSvc(1, false),
                            preporukeSvc(false)
                                    .delayElement(Duration.ofSeconds(3)))   // jako sporo
                    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
                    .timeout(Duration.ofMillis(500))                         // hard cap
                    .block();
        } catch (Exception ex) {
            log("greška", "overall timeout: " + ex.getClass().getSimpleName()
                    + " posle " + (System.currentTimeMillis() - t0) + "ms");
        }
    }

    // -------------------------------------------------------------------
    // firstWithValue: imamo 2 izvora istog tipa (npr. primarni i
    // mirror), uzmi onaj koji prvi odgovori sa VALUE-om (ne greška).
    //
    // Tipičan slučaj: read-from-cache-or-db, "primary or backup region".
    //
    // Razlika od firstWithSignal: firstWithSignal uzima prvi BILO KAKAV
    // signal (uključujući error). firstWithValue ignoriše greške -
    // čeka prvi uspeh.
    // -------------------------------------------------------------------
    static void primer5_firstWithValue() {
        long t0 = System.currentTimeMillis();

        Mono<String> primarni = Mono.delay(Duration.ofMillis(150))
                .map(t -> "primarni-odgovor")
                .doOnSubscribe(s -> log("call", "primarni"));

        Mono<String> mirror = Mono.delay(Duration.ofMillis(80))
                .map(t -> "mirror-odgovor")
                .doOnSubscribe(s -> log("call", "mirror"));

        String rez = Mono.firstWithValue(primarni, mirror).block();

        log("rezultat", rez + " (mirror je brži - " + (System.currentTimeMillis() - t0) + "ms)");
    }

    // -------------------------------------------------------------------
    // Kompletan obrazac - sve naučeno spojeno.
    //
    // Po servisu:
    //   - retry za prolazne greške (IOException, TimeoutException)
    //   - sopstveni timeout
    //   - sopstveni fallback
    //
    // Plus overall timeout kao gvozdeni cap.
    //
    // Bonus: subscribeOn na boundedElastic-u jer naši simulirani
    // servisi koriste Thread.sleep (blocking).
    // -------------------------------------------------------------------
    static void primer6_resilientDashboard() {
        long t0 = System.currentTimeMillis();

        Retry standardni = Retry.backoff(2, Duration.ofMillis(100))
                .jitter(0.5)
                .filter(ex -> ex instanceof IOException
                           || ex instanceof TimeoutException);

        Dashboard d = Mono.zip(
                        profilSvc(1, false)
                                .timeout(Duration.ofMillis(300))
                                .retryWhen(standardni)
                                .onErrorReturn(Profil.UNKNOWN),
                        narudžbineSvc(1, true)
                                .timeout(Duration.ofMillis(300))
                                .retryWhen(standardni)
                                .onErrorReturn(List.of()),
                        preporukeSvc(true)
                                .timeout(Duration.ofMillis(200))
                                .retryWhen(standardni)
                                .onErrorReturn(List.of()))
                .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
                .timeout(Duration.ofSeconds(1))
                .block();

        log("dashboard", d);
        log("vreme", (System.currentTimeMillis() - t0) + "ms (sve sa retry-jem i fallback-om)");
    }

    // -------------------------------------------------------------------
    // Simulirani servisi.
    //
    // subscribeOn(boundedElastic) jer Thread.sleep blokira - ne smemo
    // na default schedule-u (parallel je za CPU-bound, ne blocking).
    // -------------------------------------------------------------------

    static Mono<Profil> profilSvc(int userId, boolean pada) {
        return Mono.fromCallable(() -> {
                    sleep(50);
                    if (pada) throw new IOException("profil-pad");
                    return new Profil(userId, "Marko");
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    static Mono<List<String>> narudžbineSvc(int userId, boolean pada) {
        return Mono.fromCallable(() -> {
                    sleep(120);
                    if (pada) throw new IOException("narudžbine-pad");
                    return List.of("nar-1", "nar-2");
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    static Mono<List<String>> preporukeSvc(boolean pada) {
        return Mono.fromCallable(() -> {
                    sleep(200);
                    if (pada) throw new IOException("preporuke-pad");
                    return List.of("prep-1", "prep-2", "prep-3");
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
