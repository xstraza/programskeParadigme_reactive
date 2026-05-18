package raf.edu.week1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Nedelja 1 - Push vs. Pull model
 *
 * Cilj: konceptualno pokazati razliku između dva načina dostavljanja
 * podataka:
 *  1. PULL - potrošač sam vuče elemente kad mu je zgodno (Iterator).
 *  2. PUSH - izvor gura elemente potrošaču čim su dostupni
 *     (klasičan Observer pattern, prethodnica reaktivnog modela).
 *
 * Ovo nije prava reaktivna implementacija - već minimalni mentalni
 * model. Pravi push sa backpressure-om vidimo u sledećim demo
 * klasama (Reactive Streams / Java Flow API).
 */
public class PushVsPullModel {

    public static void main(String[] args) {
        System.out.println("=== Pull model - Iterator vuče elemente ===\n");
        pullModel();

        System.out.println("\n=== Push model - Subject gura elemente ===\n");
        pushModel();

        System.out.println("\n=== Pseudo-reaktivni push sa kompozicijom ===\n");
        compositionPushModel();
    }

    // -----------------------------------------------------------------------
    // Pull model - klasičan Iterator. Potrošač diktira tempo:
    //   "daj mi sledeći", "daj mi sledeći", ...
    // Ako je izvor spor, potrošač čeka. Ako je potrošač spor, izvor "čeka".
    // -----------------------------------------------------------------------
    static void pullModel() {
        List<String> izvor = List.of("Beograd", "Niš", "Novi Sad", "Kragujevac");

        Iterator<String> it = izvor.iterator();
        while (it.hasNext()) {
            String grad = it.next();          // <-- POTROŠAČ vuče
            System.out.println("[Pull] Dobio: " + grad);
        }
    }

    // -----------------------------------------------------------------------
    // Push model - minimalni Subject. Potrošač se PRIJAVLJUJE,
    // a izvor mu kasnije gura elemente kad god ih ima. Tipičan Observer
    // pattern, dobro poznat iz GUI sveta (button click listener).
    // -----------------------------------------------------------------------
    static void pushModel() {
        Subject<String> izvor = new Subject<>();

        // Pretplata - izvor sad zna ko sluša.
        izvor.subscribe(grad -> System.out.println("[Push] Dobio: " + grad));

        // Izvor po svom tempu emituje elemente.
        izvor.emit("Beograd");
        izvor.emit("Niš");
        izvor.emit("Novi Sad");
        izvor.emit("Kragujevac");
    }

    // -----------------------------------------------------------------------
    // Pseudo-reaktivni push - Subject sa map operatorom.
    // Pokazuje da se isti operatori koje znamo iz Stream-a
    // mogu napisati i nad push izvorom - to je suština reaktivnog modela.
    // -----------------------------------------------------------------------
    static void compositionPushModel() {
        Subject<Integer> brojevi = new Subject<>();

        brojevi
                .map(n -> n * n)              // T → R operator (kao Stream.map)
                .filter(n -> n > 10)          // Predicate operator (kao Stream.filter)
                .subscribe(n -> System.out.println("[Reactive-ish] Kvadrat > 10: " + n));

        // Emisija - sad svaka emisija prolazi kroz pipeline.
        for (int i = 1; i <= 6; i++) {
            brojevi.emit(i);
        }
    }

    // -----------------------------------------------------------------------
    // Subject - minimalni push izvor sa operatorima.
    // Nije Reactive Streams kompatibilan (nema backpressure ni
    // onComplete / onError signale) - ali pokazuje ideju.
    // -----------------------------------------------------------------------
    static class Subject<T> {
        private final List<Consumer<T>> subscribers = new ArrayList<>();

        void subscribe(Consumer<T> subscriber) {
            subscribers.add(subscriber);
        }

        void emit(T value) {
            for (Consumer<T> sub : subscribers) {
                sub.accept(value);
            }
        }

        // Operator map: vraća NOVI Subject u kojem je svaki element transformisan.
        <R> Subject<R> map(java.util.function.Function<T, R> mapper) {
            Subject<R> rezultat = new Subject<>();
            this.subscribe(value -> rezultat.emit(mapper.apply(value)));
            return rezultat;
        }

        // Operator filter: vraća NOVI Subject u kome prolaze samo elementi
        // koji zadovoljavaju Predicate.
        Subject<T> filter(java.util.function.Predicate<T> predicate) {
            Subject<T> rezultat = new Subject<>();
            this.subscribe(value -> {
                if (predicate.test(value)) {
                    rezultat.emit(value);
                }
            });
            return rezultat;
        }
    }
}
