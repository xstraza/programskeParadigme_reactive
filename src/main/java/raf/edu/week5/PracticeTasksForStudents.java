package raf.edu.week5;

/**
 * Nedelja 5 - zadaci za samostalnu vežbu.
 *
 * Tema: backpressure, error handling i retry.
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
    // Zadatak 1 - onBackpressureDrop
    //
    // Producer emituje 100 tikova svakih 1ms. Consumer obrađuje
    // jedan element na svakih 30ms. Koristi onBackpressureDrop tako
    // da se consumer ne uguši i ispiši vrednosti koje stignu do njega.
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 - bounded buffer
    //
    // Isti brzi producer (200 elemenata, 1ms tick) i spori consumer
    // (sleep 30ms). Postavi onBackpressureBuffer sa maxSize=5 i
    // strategijom DROP_OLDEST. Izbroji koliko je elemenata izbačeno
    // i koliko obrađeno - zbir treba da bude 200.
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 - limitRate
    //
    // Flux.range(1, 100) sa limitRate(10). Pomoću doOnRequest(...)
    // ispiši veličine request-a koji idu ka upstream-u.
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 - onErrorReturn
    //
    // Napravi Mono<String> koji baca exception. Pomoću onErrorReturn
    // vrati string "fallback" umesto greške i odštampaj rezultat.
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 - cascading fallback
    //
    // Tri Mono<String>-a:
    //   primary - baca grešku
    //   cache   - baca grešku
    //   def     - vraća "default-val"
    //
    // Sastavi pipeline koji prvo pokušava primary, pa cache, pa def.
    // Rezultat treba da bude "default-val".
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 - skip on error
    //
    // Flux.range(1, 10). Za svaki parni broj obrada baca grešku.
    // Napravi pipeline koji u izlazu ima SAMO neparne brojeve
    // (ostatak preskoči, ne propagira grešku).
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 - retry(n)
    //
    // Napravi flaky Mono pomoću AtomicInteger brojača: prva 3 poziva
    // bacaju exception, 4. poziv vraća vrednost. Dodaj retry(3) i
    // ispiši rezultat. Loguj svaki pokušaj.
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 - Retry.backoff sa jitter-om
    //
    // Isti flaky servis kao u zadatku 7. Umesto retry(3) koristi
    // Retry.backoff(3, Duration.ofMillis(200)).jitter(0.5).
    // Loguj timestamp svakog pokušaja preko doBeforeRetry.
    // ===================================================================
    static void zadatak8() {
        // TODO
    }

    // ===================================================================
    // Zadatak 9 - retry samo za određene greške
    //
    // Dva flaky servisa:
    //   (a) baca IOException 2 puta, pa uspeva
    //   (b) baca IllegalArgumentException uvek
    //
    // Iskoristi Retry.backoff(3, 100ms) sa .filter(...) koji propušta
    // SAMO IOException. Pokreni oba servisa kroz isti retry policy.
    // ===================================================================
    static void zadatak9() {
        // TODO
    }

    // ===================================================================
    // Zadatak 10 - kompletna resilience strategija
    //
    // Simuliraj HTTP poziv koji traje ~50-150ms i pada u 70% slučajeva
    // sa IOException. Sastavi pipeline koji:
    //   - postavlja timeout 200ms,
    //   - retry-uje do 3 puta sa Retry.backoff(3, 100ms).jitter(0.5),
    //   - retry-uje samo TimeoutException i IOException,
    //   - u krajnjem slučaju vraća "FALLBACK" preko onErrorReturn.
    //
    // Pokreni pipeline 5 puta i izbroji koliko puta je rezultat
    // bio FALLBACK.
    // ===================================================================
    static void zadatak10() {
        // TODO
    }
}
