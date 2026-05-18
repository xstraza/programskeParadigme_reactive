package raf.edu.week3;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Nedelja 3 - buffer, window, groupBy.
 *
 * Operatori koji NE menjaju vrednosti, ali menjaju STRUKTURU toka:
 * od toka pojedinacnih elemenata pravimo tok "paketa".
 *
 *   buffer       - sakuplja elemente u List i emituje listu po pravilu
 *                  (broj elemenata, vreme, oba). Flux&lt;T&gt; → Flux&lt;List&lt;T&gt;&gt;.
 *
 *   window       - kao buffer, ali umesto liste emituje UNUTRASNJI Flux.
 *                  Flux&lt;T&gt; → Flux&lt;Flux&lt;T&gt;&gt;. Bogatija semantika - mozemo
 *                  primeniti reaktivne operatore na svaki prozor.
 *
 *   groupBy      - particionise tok po kljucu. Flux&lt;T&gt; → Flux&lt;GroupedFlux&lt;K, T&gt;&gt;.
 *                  Svaki GroupedFlux je nezavisni unutrasnji tok za jednu
 *                  vrednost kljuca.
 *
 * Tipicni slucajevi:
 *   - buffer(N)         : batch upis u bazu po 100 redova
 *   - buffer(Duration)  : agregacija metrika po sekundi
 *   - window            : sliding window analiza, rolling avg
 *   - groupBy           : po-korisnik / po-tip particionisanje za
 *                          paralelnu obradu
 */
public class BufferWindowGroupByDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        System.out.println("=== 1. buffer(N) - paketici fiksne velicine ===\n");
        bufferByCount();

        System.out.println("\n=== 2. buffer(Duration) - paketici po vremenu ===\n");
        bufferByTime();

        System.out.println("\n=== 3. buffer(N, Duration) - sta god prvo ===\n");
        bufferByCountOrTime();

        System.out.println("\n=== 4. window(N) - kao buffer, ali tok od tokova ===\n");
        windowDemo();

        System.out.println("\n=== 5. groupBy - particija po kljucu ===\n");
        groupByDemo();

        System.out.println("\n=== 6. groupBy + flatMap - paralelna obrada particija ===\n");
        groupByParallelDemo();
    }

    // -------------------------------------------------------------------
    // buffer(N) - sakupi N elemenata, pa emituj listu. Kad se izvor zavrsi
    // sa "nedovrsenim" paketom, emituje i njega (kraci od N).
    //
    // Primer: 7 elemenata, buffer(3) → [1,2,3], [4,5,6], [7].
    // -------------------------------------------------------------------
    static void bufferByCount() {
        Flux.range(1, 7)
                .buffer(3)
                .doOnNext(b -> log("buffer(3)", b))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // buffer(Duration) - sve sto stigne u datom prozoru ide u jednu listu.
    //
    // Pogodno za agregaciju event tokova (npr. broj klikova po sekundi).
    // -------------------------------------------------------------------
    static void bufferByTime() {
        Flux.interval(Duration.ofMillis(80))
                .take(10)
                .buffer(Duration.ofMillis(250))
                .doOnNext(b -> log("buffer(250ms)", b))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // buffer(N, Duration) - najfleksibilnije: emituj kad se popuni N
    // ILI kad istekne vreme, sta god prvo.
    //
    // Pravi backend pattern: "saljemo batch upis kad imamo 100 redova
    // ili kad prodje 1s - koje god prvo stigne".
    // -------------------------------------------------------------------
    static void bufferByCountOrTime() {
        Flux.interval(Duration.ofMillis(80))
                .take(20)
                .bufferTimeout(5, Duration.ofMillis(250))
                .doOnNext(b -> log("bufferTimeout(5, 250ms)", b))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // window - kao buffer, ali umesto List dobijamo unutrasnji Flux.
    // Mozemo na svaki prozor primeniti reaktivne operatore (count, sum,
    // distinct, ...) i tek onda spljostiti.
    //
    // Ovde: prozor od 3, na svakom radimo count() - dobijamo sume po prozoru.
    // -------------------------------------------------------------------
    static void windowDemo() {
        Flux.range(1, 10)
                .window(3)
                .flatMap(prozor -> prozor.collectList())
                .doOnNext(b -> log("window(3).collectList", b))
                .blockLast();

        System.out.println();
        Flux.range(1, 10)
                .window(3)
                .flatMap(prozor -> prozor.reduce(0, Integer::sum))
                .doOnNext(s -> log("window(3).reduce(sum)", s))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // groupBy - particionise tok po kljucu. Vraca Flux&lt;GroupedFlux&lt;K, T&gt;&gt;.
    //
    // GroupedFlux je obican Flux koji nosi svoj kljuc (group.key()).
    // Pravi se NOVI unutrasnji tok za svaki put kad se pojavi nov kljuc.
    //
    // PAZNJA: groupBy se MORA konzumirati (svaki grupni tok mora imati
    // subscriber-a), inace ce backpressure zaglaviti pipeline. Najlakse
    // - odmah flatMap-uj svaku grupu.
    // -------------------------------------------------------------------
    static void groupByDemo() {
        Flux.just("Ana", "Aleksandar", "Marko", "Milan", "Petar", "Pavle", "Ana")
                .groupBy(ime -> ime.charAt(0))
                .flatMap(group -> group.collectList()
                        .map(list -> "slovo '" + group.key() + "' -> " + list))
                .doOnNext(v -> log("groupBy", v))
                .blockLast();
    }

    // -------------------------------------------------------------------
    // Pravi primer - particije se obradjuju PARALELNO.
    //
    // Ulaz: par-id + payload. Hocemo da svaki id obradimo nezavisno
    // (jer su id-jevi nezavisni), ali da unutar jednog id-a redosled
    // bude ocuvan.
    //
    // Strategija: groupBy(id).flatMap(grupa -> grupa.concatMap(obradi))
    //   - flatMap na spoljnjem nivou: razlicite particije paralelno.
    //   - concatMap unutar particije: redosled u particiji ocuvan.
    //
    // Ovo je standardni pattern za "ordered-per-key, parallel-across-keys"
    // (npr. event sourcing po agregat-id-u).
    // -------------------------------------------------------------------
    static void groupByParallelDemo() {
        record Dogadjaj(int id, int seq) {}

        Flux<Dogadjaj> tok = Flux.just(
                new Dogadjaj(1, 1), new Dogadjaj(2, 1),
                new Dogadjaj(1, 2), new Dogadjaj(2, 2),
                new Dogadjaj(1, 3), new Dogadjaj(2, 3)
        );

        tok
                .groupBy(Dogadjaj::id)
                .flatMap(group -> group
                        .concatMap(d -> simObrada(d)
                                .doOnNext(r -> log("particija " + group.key(), r))))
                .blockLast();
    }

    static Mono<String> simObrada(Object dogadjaj) {
        return Mono.just("obradjen: " + dogadjaj)
                .delayElement(Duration.ofMillis(80));
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-26s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
