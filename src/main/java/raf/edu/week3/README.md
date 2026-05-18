# Paradigme Programiranja - Nedelja 3
## Transformacije i kombinacije tokova

> Kompozicija reaktivnih tokova: kako više nezavisnih `Mono` / `Flux`
> izvora spojiti u jedan, i kako jedan tok "razgraditi" u mnoge.

---

## Sadržaj

1. [Uvod](#1-uvod)
2. [`flatMap`, `concatMap`, `flatMapSequential`](#2-flatmap-concatmap-flatmapsequential)
3. [Šira porodica: `flatMapMany`, `flatMapIterable`, `expand`](#3-šira-porodica-flatmapmany-flatmapiterable-expand)
4. [`switchMap` - autocomplete pattern](#4-switchmap--autocomplete-pattern)
5. [`merge`, `concat`, `mergeSequential`](#5-merge-concat-mergesequential)
6. [`zip`, `combineLatest`, `withLatestFrom`, `Mono.when`, `firstWithValue`](#6-zip-combinelatest-withlatestfrom-monowhen-firstwithvalue)
7. [`buffer`, `window`, `groupBy`](#7-buffer-window-groupby)
8. [Brza referenca - koji operator kada](#8-brza-referenca--koji-operator-kada)
9. [Šta dolazi sledeće nedelje](#9-šta-dolazi-sledeće-nedelje)
10. [Primeri koda i vežbe](#10-primeri-koda-i-vežbe)

---

## 1. Uvod

Prošle nedelje smo obradili **osnovne operatore** (`map`, `filter`,
`reduce`, ...) - sve operatore koji rade nad **jednim** tokom i ne
diraju vreme.

Sad ulazimo u operatore koji su **suština reaktivnog programiranja** i
**ne postoje u Stream API-ju**:

- transformacija jednog elementa u **drugi tok** (`flatMap` familija),
- spajanje **više tokova** u jedan (`merge`, `concat`, `zip`, ...),
- razgrađivanje toka na **paketiće** ili **grupe** (`buffer`, `window`,
  `groupBy`).

Razlog što ovog nema u Stream-u je jednostavan: `Stream` ne zna za
vreme, pa "spoji dva toka po jedan element kako stignu" nema smisla -
sve se "već desilo". U reaktivnom svetu, **kada** element stigne je
prvoklasna informacija.

> **Most ka FP delu:** kao i u FP delu, sve je deklarativno
> komponovanje. Razlika je samo što sada svaki operator ima i
> **vremensku** dimenziju, i može da bude **paralelan**.

```java
// FP (week 1 prvog semestra) - flatMap nad listom
List<Integer> rez = List.of(1, 2, 3).stream()
    .flatMap(n -> Stream.of(n, n * 10))
    .toList();   // [1, 10, 2, 20, 3, 30]

// Reactor (week 3) - flatMap nad reaktivnim tokom, sa ASYNC unutrašnjim tokom
Flux.just(1, 2, 3)
    .flatMap(n -> Mono.fromCallable(() -> "User#" + n).delayElement(Duration.ofMillis(100)))
    .subscribe(System.out::println);
```

Isti operator, ista ideja - samo što unutar `flatMap`-a sada može da
bude **mrežni poziv, baza, fajl, ili drugi reaktivni izvor**.

---

## 2. `flatMap`, `concatMap`, `flatMapSequential`

Sva tri rade istu stvar: za svaki element ulaza pozovu funkciju
`T → Publisher<R>`, dobiju unutrašnji tok i njegove rezultate "spuste"
u izlaz. Razlika je u **redosledu i paralelizmu**.

| Operator | Paralelizam | Redosled rezultata | Tipičan slučaj |
|----------|-------------|--------------------|----------------|
| `flatMap` | da (max 256 default) | **nije garantovan** | nezavisni async pozivi |
| `concatMap` | ne (serijski) | **garantovan, po ulazu** | redosled važan, npr. transakcije |
| `flatMapSequential` | da | **garantovan, po ulazu** | "Promise.all" stil |

### Vizuelno

```
Ulaz:  1 ----- 2 ----- 3 -----|
       |       |       |
       v       v       v
(unutrašnji tokovi, različitih brzina)

flatMap:           [1a, 2a, 1b, 3a, 2b, 3b]   ← pomešano
concatMap:         [1a, 1b, 2a, 2b, 3a, 3b]   ← strogo po ulazu, serijski
flatMapSequential: [1a, 1b, 2a, 2b, 3a, 3b]   ← redosled isti, ali sve teklo paralelno
```

### Kada šta

- **`flatMap`** - kada želiš maksimalan paralelizam i ne mariš za
  redosled. Primer: za listu user-id-jeva, povuci profil iz REST API-ja
  za svakog. Redosled rezultata nije važan jer ćeš ih svakako mapirati
  u `Map<id, User>` ili ih ispisati.

- **`concatMap`** - kada redosled rezultata MORA da prati redosled ulaza
  i kada **side efekti** unutrašnjeg toka (npr. upis u bazu) ne smeju
  da se preklapaju. Primer: stream domena događaja iz event-sourcing
  sistema - sledeći događaj ne sme da krene dok prethodni ne završi.

- **`flatMapSequential`** - najređi, ali važan: kada želiš paralelizam
  zbog brzine, ali ti je redosled prikaza bitan. Primer: prikaz lista
  rezultata gde svaki red zahteva async obogaćivanje - neka teku
  paralelno, ali prikaži ih u redu.

### `flatMap` + `Mono` - najčešći pattern

`flatMap` na `Mono` (ili `Flux`) sa funkcijom koja vraća `Mono`:

```java
// findUserById(int) -> Mono<User>
Flux.just(1, 2, 3)
    .flatMap(userService::findById)
    .subscribe(System.out::println);
```

Bez `flatMap`-a, sa `map`-om, dobili bi `Flux<Mono<User>>` - tok
**obećanja**, ne korisnika. `flatMap` "spljošti" jedan nivo.

> **Demo:** [`FlatMapVariants.java`](FlatMapVariants.java)

---

## 3. Šira porodica: `flatMapMany`, `flatMapIterable`, `expand`

Tri "rođaka" `flatMap`-a koji popunjavaju česte rupe u praksi.

### `flatMapMany` - `Mono<T>` → `Flux<R>`

"Jedan poziv vrati listu - hoću svaki red kao zaseban element."

```java
Mono<HttpResponse> response = httpKlijent.get("/users");

Flux<User> korisnici = response.flatMapMany(r -> Flux.fromIterable(r.items()));
```

Bez `flatMapMany`-ja morali bismo `.flatMap(r -> Flux.fromIterable(...))` -
ali tip izlaza bi bio i dalje `Mono<Flux<...>>`. `flatMapMany` direktno
"prebacuje" iz Mono u Flux svet.

### `flatMapIterable` - `Flux<T>` sa `T = Iterable<R>` → `Flux<R>`

Brži i čitljiviji nego `flatMap(x -> Flux.fromIterable(x))`. Sinhron,
bez spinanja unutrašnjeg `Publisher`-a - tako da je idealan za
"ravno prelivanje" stranica/listi.

```java
Flux<List<Item>> stranice = api.fetchStranice();

Flux<Item> sviItemi = stranice.flatMapIterable(s -> s);
```

### `expand` - rekurzivno proširivanje

Za svaki emitovani element pokrene **novi `Publisher`**, a njegove
rezultate **ponovo razgranja**. Staje kad expander vrati prazan tok.

#### Klasičan slučaj: paginacija API-ja

```java
Mono<Stranica> prva = api.fetchStranicu(0);

Flux<Stranica> sve = prva.expand(s -> s.imaSledecu()
        ? api.fetchStranicu(s.broj() + 1)
        : Mono.empty());
```

`expand` će povući stranicu 0, emitovati je, pa pokrenuti
`fetchStranicu(1)`, emitovati, pa `fetchStranicu(2)`... dok backend ne
javi "nema više". Sve to bez ručnih while petlji ili rekurzivnih
poziva.

#### Drugi slučaj: BFS po stablu

```java
Flux.just(koren)
    .expand(cvor -> Flux.fromIterable(cvor.deca()));
```

Idiom za rekurzivnu strukturu - kategorije sa pod-kategorijama, file
system, DOM stablo.

> ⚠️ `expandDeep` postoji kao DFS varijanta (depth-first); `expand` je
> BFS (svi sa istog nivoa pre nego što se siđe niže).

> **Demo:** [`FlatMapFamily.java`](FlatMapFamily.java)

---

## 4. `switchMap` - autocomplete pattern

`switchMap` je rođak `flatMap`-a sa jednom ključnom razlikom:

- **`flatMap`** drži sve aktivne unutrašnje tokove paralelno.
- **`switchMap`** kada stigne **novi** ulazni element, **otkaže**
  prethodni unutrašnji tok i pokrene novi.

```
Ulaz:    A --- B --- C ---|
         |     |     |
         v     v     v
flatMap:    rezA, rezB, rezC  (sva tri stizu)
switchMap:  (rezA i rezB otkazani)  ↓
                                rezC  (samo poslednji)
```

### Klasičan primer: search-as-you-type

Korisnik kuca u search box: `"B"`, `"Be"`, `"Beo"`, `"Beog"`. Svaki
karakter okine HTTP poziv ka backend-u za sugestije. Pre nego što
prvi poziv stigne, korisnik je kucnuo još tri slova - stari rezultati
su **irelevantni**. `switchMap` otkazuje sve osim poslednjeg.

```java
kucanjeFlux
    .switchMap(upit -> httpKlijent.fetchSugestije(upit))
    .subscribe(this::prikaziSugestije);
```

`doOnCancel` na unutrašnjem tok-u nam pokazuje kada se prethodni zaista
otkazuje - vrlo poučno za debug.

### Drugi tipičan slučaj - UI selekcija

Korisnik klikne user-a A → fetchamo profil A. Klikne user-a B → fetch
A je otkazan, pokrenut fetch B. Bez `switchMap`-a, ako su pozivi
nepredvidive brzine, profil A bi mogao da stigne *nakon* profila B i
prebriše prikaz.

### Cancellation kao prvoklasni signal

Kada `switchMap` otkaže unutrašnji tok, **`cancel` signal** putuje uz
ceo pipeline tog unutrašnjeg toka. Reactor Netty HTTP klijent
**zatvara TCP konekciju** kad dobije `cancel`. Cleanup resursa radimo
u `doOnCancel` / `doFinally`.

> **Demo:** [`SwitchMapDemo.java`](SwitchMapDemo.java)

---

## 5. `merge`, `concat`, `mergeSequential`

Spajanje **postojećih** `Flux`-eva (za razliku od `flatMap`-a, koji
DINAMIČKI proizvodi unutrašnje tokove iz elemenata).

| Operator | Paralelizam | Redosled u rezultatu |
|----------|-------------|----------------------|
| `Flux.merge(f1, f2, ...)` | da | po vremenu stizanja (interleaved) |
| `Flux.concat(f1, f2, ...)` | ne | redom: svi iz f1, pa svi iz f2 |
| `Flux.mergeSequential(...)` | da | redom: izvor f1 pa f2, iako su paralelno tekli |

```java
Flux<String> tokA = Flux.just("A1", "A2").delayElements(Duration.ofMillis(100));
Flux<String> tokB = Flux.just("B1", "B2").delayElements(Duration.ofMillis(150));

Flux.merge(tokA, tokB).subscribe(System.out::println);
//   A1, B1, A2, B2 (interleaved)

Flux.concat(tokA, tokB).subscribe(System.out::println);
//   A1, A2, B1, B2 (strogi redosled, B nije počeo dok A nije završio)
```

### Veza sa `flatMap` familijom

Nisu slučajno parovi:

| Spajanje postojećih | Dinamičko spajanje |
|---------------------|--------------------|
| `merge` | `flatMap` |
| `concat` | `concatMap` |
| `mergeSequential` | `flatMapSequential` |

```java
Flux.merge(a, b, c)   ≡   Flux.just(a, b, c).flatMap(x -> x)
```

### `mergeWith` / `concatWith` / `startWith`

Instance verzije - čitljivije kad ima glavni tok i jedan dodatak:

```java
osnova.mergeWith(dodatak)
osnova.concatWith(dodatak)
osnova.startWith("init")           // ubaci na početak
osnova.concatWithValues("done")    // ubaci na kraj
```

### Pažnja: greška prekida lanac

`concat` na prvi `onError` prekida ostatak - ostali `Flux`-evi se
nikada ne pretplate. Za "ne odustaji na prvu grešku", postoji
`Flux.concatDelayError` / `mergeDelayError`. Detalji o error handling-u u narednim nedeljama.

> **Demo:** [`MergeConcatDemo.java`](MergeConcatDemo.java)

---

## 6. `zip`, `combineLatest`, `withLatestFrom`, `Mono.when`, `firstWithValue`

Spajanje **vrednosti** iz više tokova u jedan rezultat (tuple ili
korisnička funkcija).

### `zip` - "Promise.all"

Čeka po **jedan** element iz **svakog** izvora, pravi tuple, emituje.
Brzi izvor čeka spori. Kad neki izvor završi - `zip` završi.

```java
Mono<String> profile = userService.profile();
Mono<String> posts   = userService.posts();
Mono<String> friends = userService.friends();

Mono.zip(profile, posts, friends)
    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
    .subscribe(this::prikazi);
```

Sva tri poziva idu paralelno, ukupno vreme je `max` (ne `sum`).

### `combineLatest` - UI state

Emituje **na svaku promenu BILO KOG izvora**, koristeći **najnovije
poznate** vrednosti svih ostalih. Počinje tek kad svaki izvor jednom
emituje.

```java
Flux.combineLatest(tekstInput, filterChip, sortDugme,
    (tekst, filter, sort) -> new Pretraga(tekst, filter, sort))
    .switchMap(api::pretrazi)
    .subscribe(ui::prikazi);
```

Sve tri komponente UI-ja se posmatraju - kad korisnik kucne, kad
klikne na filter, kad promeni sort, **prebroji najnovije** sve i
osveži rezultat.

### `withLatestFrom` - događaj + state

`combineLatest`-ova "asimetrična" verzija: emituje **samo** na promenu
glavnog toka; drugi tok je samo "state koji leti pored".

```java
klikoviNaSubmit
    .withLatestFrom(stanjeForme, (klik, forma) -> forma)
    .flatMap(api::submituj)
    .subscribe();
```

Kad korisnik klikne Submit - uzmi trenutnu vrednost forme i pošalji.
Bez `withLatestFrom`-a, morali bismo ručno da držimo poslednju
vrednost forme.

### `Mono.when` - "sačekaj sve, vrednosti ne zanimaju"

Kao `zip`, ali ignoriše vrednosti - vraća `Mono<Void>` koji javlja
`onComplete` tek kada **svi** izvori završe.

```java
Mono<Void> sviUpisi = Mono.when(
        repo.upisi(a),
        repo.upisi(b),
        repo.upisi(c));   // paralelno; gotovo kad zadnji završi
```

Idealan za paralelno fire-and-forget: "izvrši sve ove async operacije,
javi mi kad si gotov". Bez `Mono.when`-a, morali bismo `zip` pa
`then()`, ili `flatMap` kombinacije.

### `firstWithValue` - race, ko prvi taj prošao

`Mono.firstWithValue(m1, m2, ...)` emituje **prvu vrednost** koja
stigne iz bilo kog izvora; ostali se otkažu.

```java
Mono<Cena> brza = Mono.firstWithValue(
        provajder1.cena(artikl),
        provajder2.cena(artikl),
        provajder3.cena(artikl));   // koji prvi odgovori, taj pobedi
```

Klasičan slučaj: **redundantni pozivi ka više DC-ova** za isti
podatak - uzmi prvi koji stigne, ostatak zaboravi. Postoji i
`Flux.firstWithSignal` za prvi koji emituje **bilo koji** signal
(value ili error).

### Mini-poređenje

| Operator | Kad emituje | Šta dobija |
|----------|-------------|------------|
| `zip` | čim svi izvori imaju po novi element | tuple po jedan iz svakog |
| `combineLatest` | čim BILO KOJI izvor emituje | tuple najnovijih svih |
| `withLatestFrom` | čim GLAVNI izvor emituje | (glavni, last(drugi)) |
| `Mono.when` | čim SVI završe | `Void` (samo signal) |
| `firstWithValue` | čim PRVI emituje | vrednost prvog |

> **Demo:** [`ZipCombineLatestDemo.java`](ZipCombineLatestDemo.java)

---

## 7. `buffer`, `window`, `groupBy`

Operatori koji **menjaju strukturu** toka - od pojedinačnih elemenata
prave "paketiće" ili grupišu.

### `buffer` - paketići u listu

```java
Flux.range(1, 7).buffer(3)
//   → [1,2,3], [4,5,6], [7]

Flux.interval(Duration.ofMillis(80))
    .buffer(Duration.ofMillis(250))
//   → svake 250ms emituje listu svega što je stiglo

Flux.range(1, 20)
    .delayElements(Duration.ofMillis(50))
    .bufferTimeout(5, Duration.ofMillis(200))
//   → emit kad se napuni 5 ili istekne 200ms, šta god prvo
```

Tipičan slučaj: **batch upis u bazu**, **agregacija metrika po
sekundi**, **slanje WebSocket poruka u grupama radi efikasnosti**.

### `window` - paketići kao unutrašnji `Flux`

Isto kao `buffer`, ali umesto `List<T>` emituje `Flux<T>`. Zato možemo
**reaktivne operatore** primeniti na svaki prozor pre nego što
spljostimo:

```java
Flux.range(1, 10)
    .window(3)
    .flatMap(prozor -> prozor.reduce(0, Integer::sum))   // suma po prozoru
//   → 1+2+3, 4+5+6, 7+8+9, 10
//   → 6, 15, 24, 10
```

### `groupBy` - particionisanje po ključu

Razdeli tok u **mnogo unutrašnjih `GroupedFlux`-eva**, po jedan za
svaku vrednost ključa.

```java
Flux<Dogadjaj> tok = ...;

tok.groupBy(Dogadjaj::idKorisnika)
   .flatMap(group -> group
        .concatMap(d -> obradi(d))               // unutar grupe - redom
        .doOnNext(r -> log(group.key(), r)))     // izmedju grupa - paralelno
   .subscribe();
```

Ovo je standardni pattern za **"ordered-per-key, parallel-across-keys"**
- svaki user-id ima svoju nezavisnu obradu, ali događaji za jednog
user-a idu redom.

> ⚠️ `groupBy` se MORA konzumirati - svaki `GroupedFlux` mora imati
> subscriber-a, inače backpressure zaglavi pipeline. Najlakše:
> `flatMap` na izlazu, kao gore.

> **Demo:** [`BufferWindowGroupByDemo.java`](BufferWindowGroupByDemo.java)

---

## 8. Brza referenca - koji operator kada

### "Imam Flux, hoću za svaki element da pozovem async i da sakupim"

| Šta želim | Operator |
|-----------|----------|
| brzo, redosled nebitan | `flatMap` |
| redosled mora po ulazu, serijski | `concatMap` |
| brzo + redosled po ulazu | `flatMapSequential` |
| samo poslednji ulaz nas zanima | `switchMap` |

### "Imam Mono ili Flux<Iterable<T>> - kako da 'spljoštim'"

| Šta želim | Operator |
|-----------|----------|
| Mono → Flux (lista u jednom pozivu) | `flatMapMany` |
| Flux<List<T>> → Flux<T> (sinhrono) | `flatMapIterable` |
| rekurzivno (paginacija, BFS po stablu) | `expand` / `expandDeep` |

### "Imam više postojećih Flux/Mono - kako da spojim"

| Šta želim | Operator |
|-----------|----------|
| svi paralelno, redosled "kako stigne" | `Flux.merge` |
| jedan pa drugi pa treći (serijski) | `Flux.concat` |
| paralelno ali rezultat redom izvora | `Flux.mergeSequential` |
| čekam SVE, pa formiram kompozit | `Mono.zip` / `Flux.zip` |
| čekam SVE, vrednosti ne trebaju | `Mono.when` |
| ko prvi sa vrednošću, taj prošao | `Mono.firstWithValue` |
| svaka promena bilo gde okida emit | `Flux.combineLatest` |
| samo glavni okida, drugi je "state" | `withLatestFrom` |

### "Imam jedan Flux, hoću da ga grupišem"

| Šta želim | Operator |
|-----------|----------|
| po N elemenata u listu | `buffer(N)` |
| svake T sekundi listu | `buffer(Duration)` |
| N ili T, šta prvo | `bufferTimeout(N, T)` |
| paketići ali kao reaktivni unutrašnji tok | `window` |
| po ključu, paralelno per-ključ | `groupBy` |

---

## 9. Šta dolazi sledeće nedelje

Do sada smo komponovali tokove **bez razmišljanja o nitima**. Sve je
"nekako radilo" - `delayElements` je pokretao `Schedulers.parallel`,
`flatMap` je davao paralelizam, itd. Sledeća nedelja sve to čini
**eksplicitnim**:

| Tema | Operator |
|------|----------|
| Promena niti za downstream | `publishOn(Scheduler)` |
| Promena niti za upstream (subscribe) | `subscribeOn(Scheduler)` |
| Tipovi scheduler-a | `Schedulers.parallel`, `boundedElastic`, `single`, `immediate` |
| Eksplicitni paralelizam | `Flux.parallel().runOn(scheduler)` |

---

## 10. Primeri koda i vežbe

| Fajl | Tema |
|------|------|
| [`FlatMapVariants.java`](FlatMapVariants.java) | `flatMap` / `concatMap` / `flatMapSequential`, paralelizam i redosled |
| [`FlatMapFamily.java`](FlatMapFamily.java) | `flatMapMany` / `flatMapIterable` / `expand` (paginacija, BFS) |
| [`SwitchMapDemo.java`](SwitchMapDemo.java) | `switchMap` - autocomplete, cancellation signal |
| [`MergeConcatDemo.java`](MergeConcatDemo.java) | `merge` / `concat` / `mergeSequential` / `startWith` |
| [`ZipCombineLatestDemo.java`](ZipCombineLatestDemo.java) | `zip` / `combineLatest` / `withLatestFrom` |
| [`BufferWindowGroupByDemo.java`](BufferWindowGroupByDemo.java) | `buffer` / `window` / `groupBy` |
| [`PracticeTasksForStudents.java`](PracticeTasksForStudents.java) | Zadaci za samostalnu vežbu |
| [`PracticeTasksSolutions.java`](PracticeTasksSolutions.java) | Rešenja zadataka |

---

## Reference

- Reactor Reference Guide - [Transforming and Filtering](https://projectreactor.io/docs/core/release/reference/#which-operator) (poglavlje koji-operator-kada)
- Reactor Marble Diagrams - vizuelni dijagrami za svaki operator u [Javadoc-u](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html)
- Project Reactor course (E. Herrera) - [eherrera.net/project-reactor-course](https://eherrera.net/project-reactor-course/)

---
