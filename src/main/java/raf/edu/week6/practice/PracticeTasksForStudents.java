package raf.edu.week6.practice;

/**
 * Nedelja 6 - zadaci za samostalnu vežbu.
 *
 * Tema: praktični obrasci - HTTP klijent, event bus, state, ETL,
 * combining.
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
    // Zadatak 1 - HTTP GET sa fallback-om
    //
    // Pomoću reactor.netty.http.client.HttpClient pozovi
    //   https://jsonplaceholder.typicode.com/posts/2
    // i ispiši odgovor.
    //
    // Dodaj timeout od 2s i onErrorReturn("FALLBACK") za slučaj da
    // nema mreže ili poziv padne.
    // ===================================================================
    static void zadatak1() {
        // TODO
    }

    // ===================================================================
    // Zadatak 2 - paralelni HTTP GET-ovi
    //
    // Za ID-jeve 1..5 pozovi /posts/{id} u paraleli (flatMap sa
    // concurrency=5) i sakupi rezultate u List<String>. Ispiši
    // koliko je trajalo - treba da bude blizu vremena najsporijeg
    // poziva, ne zbira svih.
    // ===================================================================
    static void zadatak2() {
        // TODO
    }

    // ===================================================================
    // Zadatak 3 - multicast event bus
    //
    // Napravi Sinks.Many<String> sa multicast() strategijom.
    // Pretplati DVA subscriber-a, oba ispisuju primljene poruke.
    // Emituj "a", "b", "c" i complete signal. Oba subscriber-a treba
    // da vide svih 3 poruke.
    // ===================================================================
    static void zadatak3() {
        // TODO
    }

    // ===================================================================
    // Zadatak 4 - replay sa kasnim subscriber-om
    //
    // Sinks.Many<Integer> sa replay().limit(5).
    // Emituj brojeve 1..10. Pretplati subscriber TEK POSLE - on treba
    // da vidi brojeve 6..10 (poslednjih 5).
    // ===================================================================
    static void zadatak4() {
        // TODO
    }

    // ===================================================================
    // Zadatak 5 - brojač pomoću scan-a
    //
    // Sinks.Many<Integer> kao kanal akcija (+1 ili -1).
    // Pomoću scan(0, Integer::sum) napravi stream "trenutno stanje
    // brojača". Subscriber ispisuje svako novo stanje.
    //
    // Emituj: +1, +1, +1, -1, +1. Stanja: 1, 2, 3, 2, 3.
    // ===================================================================
    static void zadatak5() {
        // TODO
    }

    // ===================================================================
    // Zadatak 6 - Redux-stil TODO lista
    //
    // sealed interface Action permits Add, Toggle, Remove
    //   - Add(String text)
    //   - Toggle(int id)
    //   - Remove(int id)
    //
    // record TodoItem(int id, String text, boolean done)
    //
    // Reducer čuva List<TodoItem>. Add dodaje novi (id = size + 1),
    // Toggle invertuje done na zadatom id-u, Remove ga uklanja.
    //
    // Emituj akcije i ispisuj stanje posle svake. Koristi
    // distinctUntilChanged.
    // ===================================================================
    static void zadatak6() {
        // TODO
    }

    // ===================================================================
    // Zadatak 7 - ETL sa batching-om
    //
    // Flux.range(1, 100) kao izvor. Za svaki broj napravi
    // "obradi" Mono koji simulira 20ms posla i vraća n*n.
    // Sakupi rezultate u batch-eve od po 10 i "bulk upiši" (ispiši listu).
    //
    // Cilj: vidi se 10 ispisa po 10 rezultata.
    // ===================================================================
    static void zadatak7() {
        // TODO
    }

    // ===================================================================
    // Zadatak 8 - error isolation u ETL-u
    //
    // Flux.range(1, 20). Za svaki paran broj parseRow baca exception.
    // Sastavi pipeline koji:
    //   - paralelno obrađuje (concurrency=4),
    //   - preskače loše elemente (ne ruši ceo tok),
    //   - sakuplja u listu i ispisuje uspešne + broj odbačenih.
    // ===================================================================
    static void zadatak8() {
        // TODO
    }

    // ===================================================================
    // Zadatak 9 - paralelni dashboard sa zip-om
    //
    // Tri Mono<String>-a simulator servisa:
    //   svcA - vraća "A" posle 100ms
    //   svcB - vraća "B" posle 150ms
    //   svcC - vraća "C" posle 200ms
    //
    // Sastavi Mono.zip(a, b, c) i mapiraj u jedan String "A|B|C".
    // Izmeri vreme - treba da bude ~200ms, ne 450ms.
    // ===================================================================
    static void zadatak9() {
        // TODO
    }

    // ===================================================================
    // Zadatak 10 - resilient dashboard
    //
    // Tri Mono-a:
    //   svcA - uspeva, 50ms
    //   svcB - baca IOException uvek
    //   svcC - traje 800ms (skoro pa timeout)
    //
    // Sastavi pipeline koji:
    //   - po servisu daje timeout 300ms i onErrorReturn("DEF-X"),
    //   - svuda probaš retry policy backoff(2, 50ms) sa filter samo
    //     za IOException/TimeoutException,
    //   - overall timeout 1s,
    //   - na kraju ispiše rezultat "A=X B=Y C=Z".
    //
    // Cilj: vidi se da B i C imaju default vrednosti, ali rezultat
    // ipak dolazi.
    // ===================================================================
    static void zadatak10() {
        // TODO
    }
}
