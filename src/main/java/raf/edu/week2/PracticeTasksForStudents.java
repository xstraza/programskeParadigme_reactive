package raf.edu.week2;

/**
 * Nedelja 2 - zadaci za samostalnu vežbu.
 *
 * Svaki zadatak ima:
 *   - kratak opis (šta treba)
 *   - "očekivani izlaz" - približno kako bi trebalo da izgleda ispis
 *   - prazno mesto za rešenje (TODO)
 *
 * Rešenja su u {@link PracticeTasksSolutions}.
 *
 * Pokreni odgovarajuću metodu iz main-a (otkomentariši red), ili
 * kopiraj svaki zadatak u svoj main da radiš pojedinačno.
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
    }

    // ===================================================================
    // Zadatak 1 - Flux iz liste imena
    //
    // Napravi Flux od liste imena ["Ana", "marko", "PETAR", "Jovana"].
    // Pretvori sva imena u "Title Case" (prvi karakter veliko, ostalo
    // malo), filtriraj samo ona koja imaju 4+ karaktera, i ispiši ih.
    //
    // Očekivani izlaz:
    //   Marko
    //   Petar
    //   Jovana
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 - kvadrati neparnih brojeva
    //
    // Iz Flux.range(1, 20):
    //   - filtriraj samo neparne
    //   - kvadriraj svaki
    //   - sumiraj rezultat (reduce)
    //   - ispiši konačnu sumu
    //
    // Očekivani izlaz: 1330
    // (1² + 3² + 5² + ... + 19² = 1330)
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 - fromCallable vs just
    //
    // Pravilo iz README-a: just se izvršava ODMAH; fromCallable na
    // svaki subscribe.
    //
    // (a) Napravi Mono.just(System.currentTimeMillis()).
    // (b) Napravi Mono.fromCallable(() -> System.currentTimeMillis()).
    //
    // Subscribe-uj svaki tri puta sa malim sleep-om između (50ms),
    // i ispiši dobijene vrednosti.
    //
    // Šta primećuješ?
    //   - just: sve tri vrednosti su iste (vreme uhvaćeno pri kreiranju)
    //   - fromCallable: tri različite vrednosti (svaki subscribe = nov poziv)
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 - vremenski razvučen tok sa logom
    //
    // Napravi Flux.range(1, 5), dodaj delayElements(300ms), i loguj
    // svaki element u formatu:
    //   [HH:mm:ss.SSS] (thread) -> N
    //
    // Koristi block-/blockLast da main čeka.
    //
    // Pitanje za razmišljanje: na kojoj je niti tok? (pogledaj naziv niti)
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 - svi doOn* hooks
    //
    // Napravi Flux.just("a", "b", "c") i okači REDOM:
    //   doOnSubscribe, doOnRequest, doOnNext, doOnComplete,
    //   doFinally
    //
    // Svaki hook neka ispiše svoj naziv i šta je primio.
    // Subscribe-uj.
    //
    // Pitanje: koji je redosled ispisa?
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 - error tok sa log() i fallback-om
    //
    // (a) Napravi Mono.error(new RuntimeException("oops")).
    // (b) Dodaj log() operator.
    // (c) Dodaj defaultIfEmpty("default") - pitanje: da li se okida na error?
    // (d) Probaj umesto toga onErrorReturn("fallback") (videti Reactor docs).
    //
    // Cilj zadatka je da vidiš RAZLIKU između "prazan tok" (defaultIfEmpty)
    // i "error tok" (onErrorReturn). To su dva različita signala.
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 - fallback chain sa switchIfEmpty
    //
    // Simuliraj 3 izvora podataka (kes, baza, API) - svaki je metoda
    // koja vraća Mono<String>. Prva dva neka budu Mono.empty(), treći
    // neka vrati realno ime.
    //
    // Sastavi pipeline koji ide kes -> baza -> API i ispiši rezultat.
    //
    // Bonus: dodaj doOnNext na svakom koraku da se vidi koji je izvor
    // "vratio" vrednost (i da je samo treći zaista emitovao).
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 - generate, sa stanjem (Fibonacci do 100)
    //
    // Koristeći Flux.generate, napravi tok Fibonacci brojeva
    // (1, 1, 2, 3, 5, 8, 13, 21, ...). Završi tok kada element pređe
    // 100 (sink.complete()).
    //
    // Sakupi rezultat u List i ispiši.
    //
    // Očekivani izlaz: [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    // ===================================================================
    static void zadatak8() {
        // TODO
    }
}
