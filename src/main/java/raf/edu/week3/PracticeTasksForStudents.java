package raf.edu.week3;

/**
 * Nedelja 3 — zadaci za samostalnu vežbu.
 *
 * Tema: transformacije i kombinacije tokova
 *   - flatMap / concatMap / switchMap
 *   - merge / concat
 *   - zip / combineLatest
 *   - buffer / groupBy
 *
 * Svaki zadatak ima:
 *   - kratak opis (šta treba)
 *   - "očekivani izlaz" — približno kako bi trebalo da izgleda ispis
 *   - prazno mesto za rešenje (TODO)
 *
 * Rešenja su u {@link PracticeTasksSolutions}.
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
    // Zadatak 1 — flatMap sa Mono (fetch by id)
    //
    // Dat je Flux<Integer> userIds = Flux.just(1, 2, 3, 4, 5).
    // Napiši pomoćnu metodu Mono<String> fetchUser(int id) koja simulira
    // async poziv (delay 100ms) i vraća "User#<id>".
    //
    // Za svaki id pozovi fetchUser i ispiši rezultat. Redosled nije bitan.
    //
    // Tehnike: flatMap, Mono.fromCallable / Mono.just + delayElement.
    //
    // Očekivani izlaz: 5 redova "User#1".."User#5" (pomešani redosled je OK).
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 — concatMap (redosled bitan)
    //
    // Isti zadatak kao 1, ali sada redosled MORA biti 1..5.
    // Zameni flatMap sa concatMap. Probaj oba i uveri se da je razlika
    // vidljiva (ako su dovoljno raznoliki delay-i).
    //
    // BONUS: koristi varijantu Mono.delay sa nasumicnim trajanjem
    // (npr. 50..300ms) i pokreni više puta, da vidiš da redosled
    // ostaje stabilan.
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 — switchMap (autocomplete)
    //
    // Simuliraj korisnika koji kuca: tok upita ["B", "Be", "Beo", "Beog"]
    // sa razmakom 50ms. Za svaki upit napravi Mono koji vraća
    // "rezultat za <upit>" sa delay-em 200ms.
    //
    // Cilj: u rezultatu da vidiš SAMO "rezultat za Beog" — sve ostalo
    // je otkazano. Koristi doOnCancel da dodatno potvrdiš.
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 — Promise.all sa Mono.zip
    //
    // Simuliraj 3 nezavisna async poziva:
    //   - profile()  -> Mono<String>, traje 300ms, vraća "profile"
    //   - posts()    -> Mono<String>, traje 500ms, vraća "posts"
    //   - friends()  -> Mono<String>, traje 200ms, vraća "friends"
    //
    // Pomoću Mono.zip pokupi sve tri vrednosti, formiraj string
    // "Dashboard: profile + posts + friends" i ispiši ga.
    //
    // Ukupno trajanje treba da bude ~500ms (max), ne 1000ms (sum).
    // Izmeri vreme System.currentTimeMillis pre i posle .block().
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 — combineLatest (UI state)
    //
    // Simuliraj dva UI input-a:
    //   - textInput: Flux.just("a", "ab", "abc") sa delay 200ms
    //   - filter:    Flux.just("ALL", "ACTIVE") sa delay 350ms
    //
    // Pomoću combineLatest emituj string formata
    // "search(text=<X>, filter=<Y>)" SVAKI PUT kada se BILO KOJI od
    // dva izvora promeni.
    //
    // Pitanje: zašto ne vidiš ispis dok BAR JEDAN element ne stigne
    // iz svakog izvora?
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 — merge dva event toka
    //
    // Imamo dva nezavisna izvora događaja:
    //   - "klikovi":  Flux.interval(150ms) sa take(5), mapiran u
    //                 "klik-N" string
    //   - "tasteri":  Flux.interval(220ms) sa take(5), mapiran u
    //                 "taster-N"
    //
    // Spoji ih u jedan tok pomoću merge i ispiši svaki događaj sa
    // vremenom (LocalTime.now()).
    //
    // Pitanje: koliko ukupno događaja se ispiše? Da li je redosled
    // garantovan?
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 — buffer (batch obrada)
    //
    // Simuliraj tok od 20 stavki (Flux.range(1, 20)) sa delay 50ms.
    // Grupiši ih u "batch"-ove od po 5 i ispiši svaki batch kao List.
    //
    // Onda promeni — grupiši po VREMENU (buffer(Duration.ofMillis(200)))
    // i uporedi rezultate.
    //
    // Pitanje: koja od dve varijante je pogodnija za "saljemo batch upis
    // svakih 200ms ili kad nakupimo 5 stavki — što god prvo"? (odgovor:
    // bufferTimeout)
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 — groupBy + paralelna obrada
    //
    // Dat je Flux<String> rec = Flux.just(
    //     "ana", "ALEKSANDAR", "Marko", "milan", "PETAR", "pavle", "Jovana"
    // );
    //
    // Grupiši po prvom slovu (case-insensitive, koristi
    // Character.toLowerCase). U svakoj grupi pretvori reč u UPPER CASE
    // i sakupi u listu.
    //
    // Ispiši: "slovo X -> [REC1, REC2, ...]" za svaku grupu.
    //
    // BONUS: dodaj .doOnNext u svakoj particiji da vidiš da li se
    // grupe obrađuju paralelno ili serijski. (Bez subscribeOn — to je
    // tema nedelje 4 — sve je na jednoj niti.)
    // ===================================================================
    static void zadatak8() {
        // TODO
    }
}
