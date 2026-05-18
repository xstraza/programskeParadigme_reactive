package raf.edu.week4;

/**
 * Nedelja 4 - zadaci za samostalnu vežbu.
 *
 * Tema: schedulers, paralelizam i tajming
 *   - Schedulers.parallel / boundedElastic / single
 *   - subscribeOn - biranje niti za upstream
 *   - publishOn   - biranje niti za downstream
 *   - blocking pozivi i kako ih bezbedno spakovati
 *   - polling pattern (interval + blocking)
 *   - timeout sa fallback Publisher-om
 *   - Flux.parallel().runOn(...).sequential()
 *
 * Saveti:
 *   - Uvek loguj Thread.currentThread().getName() da bi se VIDELE niti.
 *   - Tok ne radi ništa dok ne pozoveš .subscribe(...) ili .block(...).
 *   - delayElements / interval interno koriste parallel pool.
 */
public class PracticeTasksForStudents {

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
    // Zadatak 1 - prepoznaj nit
    //
    // Napravi Flux od 5 brojeva. Za svaki element ispiši vrednost i ime
    // niti na kojoj se izvršava.
    //
    // Pokreni isti tok dva puta: jednom bez subscribeOn-a, jednom sa
    // subscribeOn na boundedElastic-u.
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 - mesto subscribeOn-a u lancu
    //
    // Napravi tok od 3 elementa sa par doOnNext-ova koji loguju nit.
    // Pokreni ga tri puta, sa subscribeOn(boundedElastic) na različitim
    // pozicijama u lancu: na početku, na sredini, na kraju.
    //
    // Cilj zadatka: pokazati da pozicija subscribeOn-a u
    // lancu ne menja rezultat.
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 - publishOn segmentira lanac
    //
    // Napravi tok od 3 elementa sa tri segmenta razdvojena sa dva
    // publishOn-a. U svakom segmentu loguj nit.
    //
    // Iskoristi dva različita scheduler-a (npr. parallel i
    // boundedElastic) da se jasno vidi prebacivanje.
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 - blocking poziv spakovan u Mono
    //
    // Napravi pomoćnu metodu koja simulira blocking poziv (Thread.sleep
    // ~200ms i vraća String).
    //
    // Iskoristi je u Mono na pravilan način (boundedElastic). Loguj nit
    // unutar blocking metode.
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 - implicitna nit kod vremenskih operatora
    //
    // Napravi tok sa doOnNext PRE i POSLE delayElements-a. Loguj nit u
    // oba. Pokreni i uporedi imena niti pre i posle.
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 - polling pattern
    //
    // Sastavi polling pipeline koji:
    //   - okida na svakih ~400ms (Flux.interval)
    //   - ograniči na 3 emisije
    //   - za svaki tick pozove blocking metodu koja vraća status string
    //   - blocking metoda mora ići na pravu nit
    //
    // Loguj nit unutar blocking metode.
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 - timeout sa fallback Publisher-om
    //
    // Napravi dva blocking izvora:
    //   - primarni: spor (npr. 500ms)
    //   - cache:    brz   (npr. 50ms)
    //
    // Spakuj svaki na pravilan način. Pomoću timeout-a sa fallback
    // Mono-om napravi pipeline koji preuzima cache ako primarni ne
    // stigne na vreme.
    //
    // Pokreni i sa kratkim timeout-om (cache pobeđuje) i sa dugim
    // timeout-om (primarni pobeđuje), pa uporedi rezultate.
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 - CPU heavy posao na više jezgara
    //
    // Napravi sinhronu CPU heavy metodu (busy loop ~150ms koji vraća
    // neki int).
    //
    // Pokreni je nad tokom od 8 brojeva:
    //   (a) sekvencijalno (običan map),
    //   (b) paralelno na više rail-ova (Flux.parallel + runOn).
    //
    // Izmeri trajanje obe varijante i uporedi.
    // ===================================================================
    static void zadatak8() {
        // TODO
    }

    // ===================================================================
    // Zadatak 9 - zip više paralelnih blocking poziva
    //
    // Simuliraj 3 nezavisna blocking poziva sa različitim trajanjem
    // (npr. 200, 300, 500 ms).
    //
    // Pokreni ih PARALELNO (svaki na pravom scheduler-u) i kombinuj
    // rezultate u jedan composite string.
    //
    // Izmeri ukupno trajanje - cilj je da bude blizu najsporijeg
    // poziva, ne suma svih.
    // ===================================================================
    static void zadatak9() {
        // TODO
    }

    // ===================================================================
    // Zadatak 10 - kombinovani pipeline (I/O + CPU)
    //
    // Napravi pipeline koji simulira realan ETL:
    //   - generiše brojeve 1..10
    //   - za svaki broj pozove blocking metodu koja vraća string
    //     (paralelizam ograniči na 3 istovremena poziva)
    //   - rezultat (string) propusti kroz CPU heavy transformaciju
    //     (npr. teški upper-case + neki busy loop)
    //   - sakupi sve u listu
    //
    // Loguj nit u svakoj fazi - cilj je da se vidi prebacivanje između
    // I/O i CPU pool-a.
    // ===================================================================
    static void zadatak10() {
        // TODO
    }
}
