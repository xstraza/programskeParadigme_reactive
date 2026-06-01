package raf.edu.week6.eventbus;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * Nedelja 6 - event bus pomoću Sinks.many().
 *
 * Sinks su moderni, type-safe most između push-izvora (npr. listener
 * koji dobija notifikacije, broker, UI događaj) i reaktivnog tipa
 * (Flux).
 *
 *   Sinks.many().unicast()                      - 1 subscriber
 *   Sinks.many().multicast().onBackpressureBuffer()  - 1:N, samo novi
 *   Sinks.many().replay().all()                  - 1:N, sve od početka
 *   Sinks.many().replay().limit(N)               - 1:N, poslednjih N
 *   Sinks.many().replay().latest()               - 1:N, samo zadnji
 *
 * Producer strana: bus.emitNext / emitComplete / emitError.
 * Consumer strana: bus.asFlux().subscribe(...).
 */
public class EventBusDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. UNICAST - tačno jedan subscriber ===\n");
        primer1_unicast();

        System.out.println("\n=== 2. MULTICAST - više subscriber-a, ali samo nove poruke ===\n");
        primer2_multicast();

        System.out.println("\n=== 3. REPLAY - kasniji subscriber dobija istoriju ===\n");
        primer3_replay();

        System.out.println("\n=== 4. Konkurentne emisije - zašto postoji EmitFailureHandler ===\n");
        primer4_konkurentnaEmisija();
    }

    // -------------------------------------------------------------------
    // UNICAST: ugovor za "tačno jedan subscriber". Drugi pokušaj
    // pretplate vraća grešku.
    //
    // Tipičan slučaj: kanal poruka između dva interna kompo­nenta -
    // jedan piše, jedan čita, niko treći.
    // -------------------------------------------------------------------
    static void primer1_unicast() {
        Sinks.Many<String> bus = Sinks.many().unicast().onBackpressureBuffer();

        // Prvi subscriber - prolazi.
        bus.asFlux().subscribe(v -> log("sub1", v));

        bus.emitNext("a", FAIL_FAST);
        bus.emitNext("b", FAIL_FAST);

        // Drugi subscriber - dobija grešku iz unicast-a.
        bus.asFlux().subscribe(
                v   -> log("sub2", v),
                err -> log("sub2-error", err.getMessage()));

        bus.emitNext("c", FAIL_FAST);
        bus.emitComplete(FAIL_FAST);
    }

    // -------------------------------------------------------------------
    // MULTICAST: 1:N. Svaki subscriber dobija SAMO poruke koje su
    // emitovane POSLE njegove pretplate.
    //
    // U primeru "a" i "b" su emitovani pre nego što se sub2 pretplatio
    // - on ih NE vidi. "c" stiže oboma.
    //
    // Tipičan slučaj: live notifikacije, klikovi, broker poruke.
    // -------------------------------------------------------------------
    static void primer2_multicast() {
        Sinks.Many<String> bus = Sinks.many().multicast().onBackpressureBuffer();

        bus.asFlux().subscribe(v -> log("sub1", v));

        bus.emitNext("a", FAIL_FAST);   // samo sub1
        bus.emitNext("b", FAIL_FAST);   // samo sub1

        bus.asFlux().subscribe(v -> log("sub2", v));

        bus.emitNext("c", FAIL_FAST);   // oboma
        bus.emitNext("d", FAIL_FAST);   // oboma
        bus.emitComplete(FAIL_FAST);
    }

    // -------------------------------------------------------------------
    // REPLAY.all(): kasni subscriber dobija sve OD POČETKA, čak iako
    // se pretplatio posle emisije.
    //
    // Cena: sve poruke se drže u memoriji. Za beskonačan tok - opasno.
    // Često se umesto toga koristi:
    //   replay().limit(N)       - poslednjih N
    //   replay().latest()       - samo poslednja
    //   replay().limit(Duration) - poslednjih X sekundi
    //
    // Tipičan slučaj: "open chat with last 50 messages", "current state
    // for newly connected client".
    // -------------------------------------------------------------------
    static void primer3_replay() {
        Sinks.Many<String> bus = Sinks.many().replay().limit(3);

        bus.emitNext("a", FAIL_FAST);
        bus.emitNext("b", FAIL_FAST);
        bus.emitNext("c", FAIL_FAST);
        bus.emitNext("d", FAIL_FAST);
        bus.emitNext("e", FAIL_FAST);

        log("info", "Sad se pretplaćujem - poslednjih 3 emitovanih:");

        // Kasni subscriber - dobija b,c,d,e (limit je 3 ali sub stiže pre
        // nego što complete signal isključi sve - dobija samo replay buffer).
        // U praksi sa limit(3): vidi se "c", "d", "e" (poslednje 3).
        bus.asFlux().subscribe(v -> log("kasni-sub", v));

        bus.emitNext("f", FAIL_FAST);
        bus.emitComplete(FAIL_FAST);
    }

    // -------------------------------------------------------------------
    // EmitFailureHandler postoji jer emitNext NIJE thread-safe po
    // default-u. Dva thread-a koja istovremeno zovu emitNext mogu da
    // dobiju FAIL_NON_SERIALIZED rezultat.
    //
    // FAIL_FAST: baca odmah - dobar za dev/test, lako primeti race.
    //
    // Tipičan production handler retry-uje kratko ako je samo race:
    //   (signal, result) -> result == EmitResult.FAIL_NON_SERIALIZED
    //
    // Pošto je signal jedan thread ovde, dva paralelna emitovanja
    // mogu (verovatno hoće) da bace IllegalStateException sa
    // FAIL_FAST. Mi koristimo "busy loop" handler kao demo retry-a.
    // -------------------------------------------------------------------
    static void primer4_konkurentnaEmisija() throws InterruptedException {
        Sinks.Many<Integer> bus = Sinks.many().multicast().onBackpressureBuffer();

        bus.asFlux()
                .publishOn(Schedulers.parallel())
                .subscribe(v -> log("sub", v));

        // "Busy-loop" handler - retry-uje kratko ako je race.
        Sinks.EmitFailureHandler busyLoop = (signal, result) ->
                result == Sinks.EmitResult.FAIL_NON_SERIALIZED;

        // Dva paralelna emitter-a.
        Runnable emitter = () -> {
            for (int i = 0; i < 5; i++) {
                bus.emitNext(i, busyLoop);
                try { Thread.sleep(10); } catch (InterruptedException ignored) {}
            }
        };

        Thread t1 = new Thread(emitter, "emit-1");
        Thread t2 = new Thread(emitter, "emit-2");
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        bus.emitComplete(busyLoop);
        Thread.sleep(100);   // pričekaj da subscriber pojede
    }

    // -------------------------------------------------------------------
    // Bonus: kako se Sinks koristi kao SAFE adapter ka spoljnem callback-u.
    // Primer (ne pokrećemo - samo skica):
    //
    //   Sinks.Many<Notification> bus = Sinks.many().multicast().onBackpressureBuffer();
    //   slackClient.onMessage(msg -> bus.emitNext(msg, FAIL_FAST));
    //   Flux<Notification> stream = bus.asFlux();
    //
    // Sad imamo "callback API" pretvoren u reaktivni Flux. Klasičan
    // most ka legacy svetu.
    // -------------------------------------------------------------------

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
