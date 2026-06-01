package raf.edu.week6.practice;

/**
 * Nedelja 6 - dodatni zadaci za pripremu kolokvijuma.
 *
 * Pravilo iz konvencija projekta: ako tok inače ne bi imao vremena da
 * emituje pre kraja main-a, na kraju ga "zadrži" blokirajuće - i komentar
 * zašto.
 */
public class KolokvijumPriprema {

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
    // Zadatak 1 - isti operatori kao Stream + lenjost toka   [nedelja 1]
    //
    // Dato: brojevi [1, 2, 3, 4, 5, 6].
    //
    // (a) Istom logikom - zadrži parne, pa ih kvadriraj - izračunaj
    //     rezultat DVA puta: jednom klasičnim Java Stream-om, jednom
    //     reaktivnim tokom. Ispiši oba; moraju biti identični: [4, 16, 36].
    //
    // (b) Pokaži da je reaktivni tok LENJ. Sastavi tok koji u koraku
    //     obrade ispisuje "radim <n>", ali ga PRVO samo sastavi i NE
    //     pokreni - ne sme se ispisati nijedno "radim". Tek kad ga
    //     pokreneš, pojavljuju se ispisi.
    //
    // Očekivano: (a) oba rezultata [4, 16, 36];
    //            (b) dok tok nije pokrenut nema "radim", a posle
    //                pokretanja se vidi "radim 1".."radim 6".
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 - cold izvor, transformacija i tajming   [nedelja 2]
    //
    // Dato:
    //   ["paradigme", "mono", "flux", "reaktivno", "tok", "backpressure"]
    //
    // Napravi tok koji:
    //   - propušta samo reči duže od 4 slova,
    //   - pretvara ih u velika slova,
    //   - uzima najviše prve 3 takve reči,
    //   - emituje ih razmaknuto u vremenu - po jedna na svakih 200ms.
    //
    // Pre prve emisije ispiši "START", za svaku reč ispiši samu reč, a
    // kad se tok završi ispiši "KRAJ".
    //
    // Očekivano: START -> PARADIGME -> REAKTIVNO -> BACKPRESSURE -> KRAJ
    // (tri reči razmaknute po ~200ms).
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 - spajanje tri nezavisna asinhrona izvora   [nedelja 3/4]
    //
    // Tri "servisa", svaki vraća tačno jednu vrednost posle kašnjenja:
    //   ime   -> "Ana"     posle 120ms
    //   grad  -> "Beograd" posle 200ms
    //   poeni -> 87        posle 150ms
    //
    // Pozovi sva tri tako da teku PARALELNO, sačekaj da svi stignu, pa
    // ispiši jedan red:
    //   "Ana iz grada Beograd ima 87 poena"
    //
    // Izmeri i ispiši ukupno vreme. Treba da bude blizu 200ms (najsporiji
    // poziv), a NE ~470ms (zbir sva tri).
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 - redosled kod paralelne obrade   [nedelja 3]
    //
    // Za brojeve 1..5 svaki broj pokreće asinhroni posao koji traje
    // (6 - n) * 100 ms i vraća taj isti broj. Dakle broj 1 je najsporiji
    // (500ms), a broj 5 najbrži (100ms).
    //
    // Napravi DVE verzije ispisa, jasno označene:
    //   (a) rezultati izlaze redom kako STIGNU (najbrži prvi)
    //       -> očekuje se: 5, 4, 3, 2, 1
    //   (b) čuva se REDOSLED izvora bez obzira na brzinu
    //       -> očekuje se: 1, 2, 3, 4, 5
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 - grupisanje   [nedelja 3]
    //
    // Polazni tok su brojevi 1..20.
    //   (a) Spakuj ih u grupe od po 5 uzastopnih i ispiši svaku grupu kao
    //       listu:
    //         [1, 2, 3, 4, 5]
    //         [6, 7, 8, 9, 10]
    //         [11, 12, 13, 14, 15]
    //         [16, 17, 18, 19, 20]
    //   (b) Razvrstaj iste brojeve na parne i neparne i na kraju ispiši
    //       koliko ih je u svakoj grupi:
    //         "parnih=10, neparnih=10"
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 - "search box", pobeđuje poslednji   [nedelja 3]
    //
    // Korisnik kuca u polje za pretragu; upiti stižu po jedan na svakih
    // 100ms:
    //   "r", "re", "rea", "reac"
    //
    // Za svaki upit kreće pretraga koja traje 150ms i vraća
    // "rezultat: <upit>". Ako stigne NOVIJI upit dok stara pretraga još
    // traje, stara pretraga se napušta - njen rezultat nas više ne
    // zanima.
    //
    // Očekivano: na kraju je ispisan SAMO "rezultat: reac".
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 - razdvajanje blokirajuće i CPU faze po nitima [nedelja 4]
    //
    // Napravi pipeline nad brojevima 1..4 koji:
    //   - svaki broj prvo "učita" iz sporog blokirajućeg izvora
    //     (simuliraj sa Thread.sleep(100)) - ta faza NE sme da se
    //     izvršava na 'main' niti, niti na pool-u namenjenom računanju,
    //   - zatim ga kvadrira (CPU posao) - ta faza MORA da bude na pool-u
    //     namenjenom računanju,
    //   - za svaku fazu ispiši vrednost i ime niti.
    //
    // Očekivano: iz ispisa se jasno vidi da blokirajuća i CPU faza rade
    // na RAZLIČITIM (i za to namenjenim) pool-ovima, a nijedna nije na
    // 'main'.
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 - stvarni paralelizam preko više niti   [nedelja 4]
    //
    // Za brojeve 1..8 izvrši CPU-intenzivnu transformaciju (npr. posao
    // koji traje ~200ms po broju) tako da se posao stvarno raspodeli na
    // VIŠE niti i da se izvršava istovremeno. Za svaki rezultat ispiši
    // vrednost i ime niti.
    //
    // Očekivano: u ispisu se vidi više različitih imena radnih niti, a
    // ukupno vreme je osetno kraće od zbira svih pojedinačnih poslova
    // (~8 * 200ms = 1.6s ako bi išlo serijski).
    // ===================================================================
    static void zadatak8() {
        // TODO
    }

    // ===================================================================
    // Zadatak 9 - brz izvor, spor potrošač + preskakanje grešaka [ned. 5]
    //
    // Izvor emituje brojeve 0..999 vrlo brzo (npr. jedan na svaki 1ms).
    // Potrošač je spor - obrada jednog broja traje ~50ms.
    //
    // Sastavi tok tako da memorija NE raste neograničeno: dok je potrošač
    // zauzet, čuva se samo NAJNOVIJA vrednost, a propuštene se odbacuju.
    //
    // Dodatno: ako je broj koji se obrađuje deljiv sa 7, obrada baca
    // grešku - takav element treba TIHO preskočiti, bez rušenja celog
    // toka.
    //
    // Očekivano: ispisuju se samo zaista obrađene vrednosti (videće se
    // "skakanje" brojeva), a tok preživljava do kraja.
    // ===================================================================
    static void zadatak9() {
        // TODO
    }

    // ===================================================================
    // Zadatak 10 - otporni dashboard + sabirnica i stanje   [nedelja 5/6]
    //
    // Tri servisa, svaki treba da da jednu vrednost:
    //   servisA: uspeva za 50ms        -> "A-OK"
    //   servisB: UVEK baca grešku       (simuliraj prolaznu mrežnu grešku)
    //   servisC: traje 800ms            (presporo)
    //
    // Sastavi pipeline koji:
    //   - svakom servisu daje rok od 300ms,
    //   - na prolaznu grešku ili istek roka pokušava ponovo do 2 puta, sa
    //     rastućim razmakom između pokušaja,
    //   - ako ni tada ne uspe, koristi rezervnu vrednost "N/A",
    //   - na kraju spaja sve u jedan red: "A=... B=... C=...".
    //
    // Pored toga: svaki ishod (uspeh ili rezerva) pošalji na zajedničku
    // sabirnicu događaja na koju su pretplaćena DVA posmatrača (oba
    // ispisuju ono što prime). Uporedo održavaj "tekuće stanje" - broj
    // uspeha i broj rezervi do sada - i ispiši ga posle svakog ishoda.
    //
    // Očekivano: rezultat stigne iako B i C ne uspeju (B=N/A, C=N/A),
    // oba posmatrača vide sve ishode, a stanje na kraju pokazuje
    // 1 uspeh i 2 rezerve.
    // ===================================================================
    static void zadatak10() {
        // TODO
    }
}
