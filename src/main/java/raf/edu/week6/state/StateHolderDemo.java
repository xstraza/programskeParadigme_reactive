package raf.edu.week6.state;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * Nedelja 6 - reaktivno stanje pomoću scan-a.
 * <p>
 * Ideja: stanje aplikacije se modeluje kao Flux<State>. Spolja dolaze
 * akcije (event bus), unutrašnji "reducer" pretvara (state, action) u
 * novo stanje. Po Redux/Elm šablonu.
 * <p>
 * Tri elementa:
 * 1. Action: čitljiv opis šta korisnik želi.
 * 2. Reducer: čista (state, action) -> state funkcija. Bez I/O.
 * 3. Stream stanja: scan(initialState, reducer) emituje svaki korak.
 * <p>
 * Prednost reaktivnog modela: subscriberi (UI) automatski dobijaju
 * promene - ne moramo da pišemo observer-e ručno. distinctUntilChanged
 * izbegava redundant rerender-e.
 */
public class StateHolderDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    // -------------------------------------------------------------------
    // 1. Action - šta korisnik može da uradi sa stanjem.
    //
    // sealed interface + records = "diskriminisana unija". Pattern
    // matching u reducer-u garantuje da nismo zaboravili slučaj.
    // -------------------------------------------------------------------
    sealed interface Action permits Add, Remove, Clear, SetTitle {
    }

    record Add(String item) implements Action {
    }

    record Remove(String item) implements Action {
    }

    record Clear() implements Action {
    }

    record SetTitle(String title) implements Action {
    }

    // -------------------------------------------------------------------
    // 2. State - immutable snapshot. Reducer NIKAD ne menja postojeći
    // state, već vraća novi.
    // -------------------------------------------------------------------
    record ShoppingList(String title, List<String> items) {
        static ShoppingList empty() {
            return new ShoppingList("Nova lista", List.of());
        }

        ShoppingList withTitle(String newTitle) {
            return new ShoppingList(newTitle, items);
        }

        ShoppingList add(String item) {
            List<String> n = new ArrayList<>(items);
            n.add(item);
            return new ShoppingList(title, List.copyOf(n));
        }

        ShoppingList remove(String item) {
            return new ShoppingList(title,
                    items.stream().filter(x -> !x.equals(item)).toList());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== 1. Osnovni scan kao state reducer ===\n");
        primer1_osnovniScan();

        System.out.println("\n=== 2. Redux-stil sa Sinks + sealed Action ===\n");
        primer2_reduxStil();

        System.out.println("\n=== 3. distinctUntilChanged - bez redundant rerender-a ===\n");
        primer3_distinct();

        System.out.println("\n=== 4. cache(1) - novi subscriberi dobijaju TRENUTNO stanje ===\n");
        primer4_cacheZaTrenutnoStanje();
    }

    // -------------------------------------------------------------------
    // Najprostiji slučaj: brojač komandi (+1, +5, -2).
    //
    // scan(seed, reducer) emituje:
    //   seed (0) -> +1 = 1 -> +5 = 6 -> -2 = 4
    //
    // To je razlika od reduce - reduce bi dao samo "4" kao Mono.
    // scan emituje SVAKI međurezultat kao Flux.
    // -------------------------------------------------------------------
    static void primer1_osnovniScan() {
        Flux<Integer> komande = Flux.just(1, 5, -2);

        komande.scan(0, Integer::sum)
                .subscribe(stanje -> log("stanje", stanje));
    }

    // -------------------------------------------------------------------
    // Pun Redux obrazac:
    //
    //   sink (akcije) -> scan(initialState, reducer) -> subscriber (UI)
    //
    // Reducer je SWITCH nad sealed-interface-om - kompajler nas tera da
    // pokrijemo sve slučajeve.
    //
    // Multicast sink znači da više UI komponenata može da sluša isti
    // tok akcija - svaki gradi svoj view nad istim state-om.
    // -------------------------------------------------------------------
    static void primer2_reduxStil() throws InterruptedException {
        Sinks.Many<Action> akcije = Sinks.many().multicast().onBackpressureBuffer();

        Flux<ShoppingList> stanja = akcije.asFlux()
                .scan(ShoppingList.empty(), StateHolderDemo::reducer);

        stanja.subscribe(s -> log("ui", s));

        akcije.emitNext(new SetTitle("Pijaca - subota"), FAIL_FAST);
        akcije.emitNext(new Add("paradajz"), FAIL_FAST);
        akcije.emitNext(new Add("hleb"), FAIL_FAST);
        akcije.emitNext(new Add("mleko"), FAIL_FAST);
        akcije.emitNext(new Remove("hleb"), FAIL_FAST);
        akcije.emitNext(new Clear(), FAIL_FAST);
        akcije.emitComplete(FAIL_FAST);

        Thread.sleep(50);
    }

    static ShoppingList reducer(ShoppingList state, Action action) {
        return switch (action) {
            case SetTitle s -> state.withTitle(s.title());
            case Add a -> state.add(a.item());
            case Remove r -> state.remove(r.item());
            case Clear c -> ShoppingList.empty();
        };
    }

    // -------------------------------------------------------------------
    // distinctUntilChanged: ne emituj ako se vrednost ne menja.
    //
    // Klasik: korisnik klikne "Add 'mleko'" dva puta - reducer to vidi
    // kao dve akcije, ali stanje ostaje isto (lista već sadrži mleko;
    // ovaj naivni reducer ne dedup-uje). UI ne treba da rerendereuje.
    // distinctUntilChanged eliminiše ponavljanja.
    //
    // Bitno: koristi equals(), pa ShoppingList kao record radi out-of-box
    // (records imaju structural equality).
    // -------------------------------------------------------------------
    static void primer3_distinct() {
        Sinks.Many<Action> akcije = Sinks.many().multicast().onBackpressureBuffer();

        akcije.asFlux()
                .scan(ShoppingList.empty(), StateHolderDemo::reducer)
                .distinctUntilChanged()
                .subscribe(s -> log("ui-render", s.items()));

        akcije.emitNext(new Add("mleko"), FAIL_FAST);   // [mleko]
        akcije.emitNext(new Add("mleko"), FAIL_FAST);   // [mleko, mleko] - razlika, rerender
        akcije.emitNext(new Remove("nepostojeci"), FAIL_FAST);  // nema promene -> NEMA rerender-a
        akcije.emitNext(new Remove("nepostojeci"), FAIL_FAST);  // ista priča
        akcije.emitComplete(FAIL_FAST);
    }

    // -------------------------------------------------------------------
    // .cache(1) - novi subscriberi dobijaju POSLEDNJE stanje pri
    // pretplati (ne moraju da čekaju sledeću akciju).
    //
    // Bez cache(1): novi UI prozor bi se otvorio i video PRAZNU listu
    // dok god neko ne pošalje novu akciju. Sa cache(1): odmah dobija
    // aktuelno stanje.
    //
    // Funkcionalno slično replay(1) na Sinks-u, ali primenjeno na
    // izlazni Flux<State>, ne na ulazni sinks. Ovo je standardni
    // obrazac za "current state holder".
    // -------------------------------------------------------------------
    static void primer4_cacheZaTrenutnoStanje() {
        Sinks.Many<Action> akcije = Sinks.many().multicast().onBackpressureBuffer();

        Flux<ShoppingList> stateHolder = akcije.asFlux()
                .scan(ShoppingList.empty(), StateHolderDemo::reducer)
                .cache(1);                              // drži poslednje stanje

        // Prvi UI - sluša od početka.
        stateHolder.subscribe(s -> log("ui-1", s.items()));

        akcije.emitNext(new Add("paradajz"), FAIL_FAST);
        akcije.emitNext(new Add("luk"), FAIL_FAST);

        log("info", "Drugi UI se sad pretplaćuje - dobiće trenutno stanje:");
        stateHolder.subscribe(s -> log("ui-2", s.items()));

        akcije.emitNext(new Add("krastavac"), FAIL_FAST);   // oba UI-a vide
        akcije.emitComplete(FAIL_FAST);
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-12s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
