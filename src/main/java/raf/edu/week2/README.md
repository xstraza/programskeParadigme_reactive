# Paradigme Programiranja - Nedelja 2
## Project Reactor - `Mono` i `Flux`, kreiranje izvora, lifecycle, osnovni operatori

---

## Sadržaj

1. [Uvod - zašto Project Reactor](#1-uvod--zašto-project-reactor)
2. [`Mono<T>` vs. `Flux<T>` - kada šta](#2-monot-vs-fluxt--kada-šta)
3. [Načini kreiranja izvora](#3-načini-kreiranja-izvora)
4. [Lifecycle, signali i `doOn*` hooks](#4-lifecycle-signali-i-doon-hooks)
5. [Osnovni operatori - preklapanje sa `Stream`-om](#5-osnovni-operatori--preklapanje-sa-stream-om)
6. [Operatori kojih nema u `Stream`-u (vremenski, fallback)](#6-operatori-kojih-nema-u-stream-u-vremenski-fallback)
7. [`block()` - kad i zašto u demo kodu](#7-block--kad-i-zašto-u-demo-kodu)
8. [Šta dolazi u nedelji 3](#8-šta-dolazi-u-nedelji-3)
9. [Primeri koda i vežbe](#9-primeri-koda-i-vežbe)

---

## 1. Uvod - zašto Project Reactor

Prošle nedelje smo ručno implementirali `Publisher` i `Subscriber`
koristeći `java.util.concurrent.Flow`, da vidimo kako protokol Reactive
Streams-a zaista radi ispod haube. To je dovoljno da se *razume*
specifikacija, ali nije dovoljno da se *pišu aplikacije*.

Šta nedostaje sirovom Flow API-ju:

- **operatori** (`map`, `filter`, `flatMap`, `merge`, `zip`, `retry`,
  `timeout` ...) - sve smo morali da pišemo ručno;
- **kontrola niti** (na kojoj niti se izvršava koji deo pipeline-a);
- **vremenski operatori** (`delay`, `interval`, `timeout`);
- **utility** za kreiranje izvora iz različitih API-ja
  (`Future`, `Callable`, `Iterable`, mreža, ...).

> Reactor je biblioteka koja pokriva sve to - implementira Reactive
> Streams kontrakt i nadograđuje ga sa stotinama operatora. Sa
> Reactor-om ne pišemo Subscriber-e ručno; pišemo deklarativni
> pipeline.

```java
// Sirov Flow API (week 1) - ručno
Flow.Publisher<Integer> publisher = nasaImplementacija();
publisher.subscribe(new Flow.Subscriber<>() { /* ~30 linija */ });

// Project Reactor (week 2) - deklarativno
Flux.range(1, 10)
    .filter(n -> n % 2 == 0)
    .map(n -> n * n)
    .subscribe(System.out::println);
```

Sve što ćemo videti u nastavku semestra je nadogradnja nad ona četiri
interfejsa iz prethodne nedelje - `Publisher`, `Subscriber`, `Subscription`,
`Processor`. Reactor samo implementira specifikaciju produkciono
spremno.

---

## 2. `Mono<T>` vs. `Flux<T>` - kada šta

Reactor ima **dva** glavna tipa izvora - i to namerno. Mogli su da imaju
samo jedan (kao RxJava `Observable<T>`), ali su odlučili da razdvoje
0..1 od 0..N. Razlog je tipska tačnost: kompajler i čitač znaju da li
ima najviše jedan element ili može biti više.

| | `Mono<T>` | `Flux<T>` |
|--|-----------|-----------|
| Broj emisija | **0 ili 1** | **0..N** (može i beskonačno) |
| Završetak | `onComplete` ili `onError` | `onComplete` ili `onError` |
| Tipičan slučaj | jedan REST poziv, jedan red iz baze, broj | lista, paginacija, event tok |
| Analogija u Java-i | `Optional<CompletableFuture<T>>` | `Stream<T>` koji teče kroz vreme |

### Kad birati `Mono<T>`

- Operacija ima *tačno* jedan rezultat ili *nema* rezultata:
  - HTTP `GET /users/{id}` → `Mono<User>` (nema = 404)
  - `userRepository.count()` → `Mono<Long>`
  - `Mono<Void>` za "uradi nešto i javi kad si gotov"
- Asinhrona "nula-ili-jedna" obećanja.

### Kad birati `Flux<T>`

- Tok od više vrednosti, makar i jedne - ali *načelno* više:
  - `userRepository.findAll()` → `Flux<User>`
  - WebSocket poruke → `Flux<Message>`
  - `Flux.interval(...)` - beskonačan tok tikova
- Kad ne znaš unapred koliko vrednosti dolazi.

### Konverzije između njih

```java
// Flux -> Mono (uzmi prvi element kao Mono, ili završi prazno)
Mono<Integer> prvi = Flux.range(1, 5).next();

// Flux -> Mono (sakupi sve u List, vrati Mono<List>)
Mono<List<Integer>> svi = Flux.range(1, 5).collectList();

// Mono -> Flux (samo "promovišemo" tip)
Flux<Integer> kaoFlux = Mono.just(42).flux();
```

> **Demo:** [`MonoVsFluxDemo.java`](MonoVsFluxDemo.java)

---

## 3. Načini kreiranja izvora

Reactor ima desetine factory metoda. Većinu je dovoljno znati po imenu -
kad zatreba, IDE auto-complete pokaže ostalo. Ovde sortirano po grupama
po *vrsti* izvora, ne po API-ju.

### 3.1. Iz statičkih, već poznatih vrednosti

```java
Mono.just("zdravo");                          // Mono<String> sa jednom vrednošću
Mono.justOrEmpty(null);                       // sigurnije od just(null) - pravi prazan Mono
Mono.empty();                                 // Mono<Void> - odmah onComplete, bez vrednosti
Mono.error(new IOException("boom"));          // Mono koji odmah onError

Flux.just(1, 2, 3, 4);                        // Flux<Integer> od 4 elementa
Flux.empty();                                 // prazan Flux
Flux.error(new RuntimeException("greska"));   // Flux koji odmah pukne
Flux.never();                                 // nikad ne emituje, nikad ne završi (za testove)
```

### 3.2. Iz postojećih Java struktura

```java
List<String> imena = List.of("Ana", "Marko", "Petar");

Flux.fromIterable(imena);                     // iz Collection / Iterable
Flux.fromArray(new Integer[]{1, 2, 3});       // iz niza
Flux.fromStream(imena.stream());              // iz Stream-a
Flux.range(1, 5);                             // 1, 2, 3, 4, 5 (count, ne endIndex!)
```

> ⚠️ `Flux.range(start, count)` - drugi parametar je **broj elemenata**,
> ne krajnja vrednost. `Flux.range(1, 5)` daje `1..5`, ne `1..4`.

### 3.3. Iz lenjih izvora (`fromCallable`, `fromSupplier`, `defer`)

Ovo je *važna grupa* - pokazuje kako Reactor gradi pojam **lenjosti**.

```java
// Sinhroni posao se NE radi sad, nego u trenutku subscribe.
Mono<String> lenj = Mono.fromCallable(() -> {
    System.out.println("Računam...");
    return skupaOperacija();              // izvršiće se tek kad neko subscribe-uje
});

// Razlika prema just(): just(skupaOperacija()) izvrši operaciju ODMAH
// (jer Java prvo evaluira argument), a tek onda Mono čeka subscribe.
Mono<String> brzo = Mono.just(skupaOperacija());   // <- skupaOperacija() je VEC pozvana
```

`defer` ide korak dalje - pravi *novi* Publisher na svakom novom subscribe-u:

```java
Mono<Long> sad = Mono.fromSupplier(System::currentTimeMillis);

// just(System.currentTimeMillis()) bi se "smrznuo" na vremenu izgradnje pipeline-a
// Mono.fromSupplier proveri vreme svaki put kad se subscribe-uje

// defer obuhvata ceo "construct" - korisno kad sami pravimo Mono unutra:
Mono<User> uvekSvez = Mono.defer(() -> userRepository.findById(id));
//                          ^^^^^ pozove se na svaki subscribe
```

> **Pravilo:** ako tvoj izvor zavisi od *trenutka subscribe*-a (vreme,
> nasumičnost, baza koja se menja), koristi `fromCallable`, `fromSupplier`
> ili `defer`. `just` je za vrednosti koje *već znaš* sad.

### 3.4. Iz `CompletableFuture` / `Future`

```java
CompletableFuture<String> future = httpKlijent.getAsync("/me");

Mono<String> mono = Mono.fromFuture(future);
// Sad imamo reaktivni tip, možemo ga lančati sa flatMap, retry, timeout, ...
```

### 3.5. Vremenski izvori

```java
// Beskonačan tok long-ova - 0, 1, 2, 3, ... svakih 100ms.
Flux<Long> tikovi = Flux.interval(Duration.ofMillis(100));

// Mono koji emituje samo jednu vrednost (0L) nakon datog vremena:
Mono<Long> kasnije = Mono.delay(Duration.ofSeconds(1));
```

> ⚠️ `interval` *podrazumevano radi na `Schedulers.parallel()`* - dakle,
> emituje na drugoj niti od main-a. Ako pokrenete demo i ne `block`-ujete
> main, JVM se ugasi pre nego što se išta vidi. Detaljno o nitima u narednim nedeljama.

### 3.6. Programsko kreiranje (`generate`, `create`)

Kada nijedan factory ne odgovara - pišemo *imperativni* generator. Postoje
dva ključna API-ja:

#### `generate` - sinhroni, jedan element po pozivu

```java
Flux<Integer> fibonacci = Flux.generate(
    () -> new int[]{0, 1},                    // početno stanje
    (state, sink) -> {
        sink.next(state[0]);                  // emituj jedan element
        int sledeci = state[0] + state[1];
        return new int[]{state[1], sledeci};  // novo stanje
    }
);
```

Pozovi `sink.next(...)` *tačno jednom* po pozivu lambde. `sink.complete()`
ako tok treba da završi. Ovaj API je idealan kad imamo "stanje koje
napreduje" (Fibonacci, brojač, čitanje iz fajla red po red, ...).

#### `create` - asinhroni, više elemenata, idealan za "premoštavanje" callback API-ja

```java
Flux<Event> dogadjaji = Flux.create(sink -> {
    listener = (event) -> sink.next(event);   // svaki event guramo u sink
    eventBus.subscribe(listener);
    sink.onCancel(() -> eventBus.unsubscribe(listener));   // cleanup
});
```

`create` se koristi kad imamo *postojeći callback API* (GUI listener,
JMS, WebSocket) i želimo da ga "obučemo" u Flux. O detaljima
backpressure-a kod `create` kasnije.

### 3.7. Tabela rezimea

| Factory | Tip | Kada |
|---------|-----|------|
| `just(T...)` | both | poznata vrednost(i), eager |
| `empty()` | both | prazan tok (samo `onComplete`) |
| `error(Throwable)` | both | tok koji odmah `onError` |
| `never()` | both | nikad ne emituje (test slučajevi) |
| `fromIterable(Iterable<T>)` | Flux | kolekcija |
| `fromArray(T[])` | Flux | niz |
| `fromStream(Stream<T>)` | Flux | postojeći Stream |
| `range(start, count)` | Flux | niz brojeva |
| `interval(Duration)` | Flux | tikovi kroz vreme |
| `delay(Duration)` | Mono | jednokratno odlaganje |
| `fromCallable(Callable<T>)` | Mono | sinhroni posao na subscribe |
| `fromSupplier(Supplier<T>)` | Mono | bez checked exception |
| `fromFuture(Future<T>)` | Mono | postojeći async posao |
| `defer(Supplier<Mono<T>>)` | both | lenjo, na svakom subscribe |
| `generate(...)` | Flux | sinhroni emitter sa stanjem |
| `create(...)` | Flux | async emitter (callback bridge) |

> **Demo:** [`KreiranjeIzvora.java`](KreiranjeIzvora.java)

---

## 4. Lifecycle, signali i `doOn*` hooks

Setimo se od prosle nedelje - svaki Subscriber prolazi kroz tačno ovaj redosled:

```
onSubscribe(Subscription)    ← uvek prvi
   ↓
onNext(elem)*                ← 0 ili više puta
   ↓
onComplete()  XOR  onError(Throwable)    ← terminalni
```

Reactor nam daje **dva načina** da se "zakačimo" za te signale:

1. **`subscribe(...)`** - terminalni, *aktivira* tok. Bez subscribe nema
   ničega.
2. **`doOn*` hooks** - međupozicije; ne aktiviraju tok, samo *posmatraju*
   signale dok prolaze.

### 4.1. `subscribe()` varijante

Reactor nudi nekoliko preklopljenih verzija:

```java
flux.subscribe();
// fire-and-forget, bez ijednog handlera. Greška se loguje na stderr.

flux.subscribe(value -> System.out.println(value));
// onNext handler.

flux.subscribe(
    value -> System.out.println(value),                  // onNext
    err   -> System.err.println("greška: " + err));      // onError

flux.subscribe(
    value -> System.out.println(value),                  // onNext
    err   -> System.err.println(err),                    // onError
    ()    -> System.out.println("gotovo"));              // onComplete

flux.subscribe(
    value -> System.out.println(value),                  // onNext
    err   -> System.err.println(err),                    // onError
    ()    -> System.out.println("gotovo"),               // onComplete
    sub   -> sub.request(Long.MAX_VALUE));               // onSubscribe - kontrola backpressure-a
```

Postoji i varijanta sa `Subscriber<T>` argumentom - tu prosleđujemo *ceo
custom subscriber* (kao u week 1 sa Flow API-jem). U praksi se retko
piše ručno, jer hooks pokrivaju 95% slučajeva.

> ⚠️ `subscribe()` vraća `Disposable` - preko koje možemo sa `.dispose()`
> da otkažemo subscription. Korisno za long-running tokove.

### 4.2. `doOn*` hooks

`doOn*` operatori su *čista observacija* - ne menjaju tok, samo izvrše
side-effect za određeni signal i puste signal dalje.

| Hook | Kada se okida |
|------|---------------|
| `doOnSubscribe(Consumer<Subscription>)` | kad neko `subscribe`-uje |
| `doOnRequest(LongConsumer)` | kad subscriber zatraži n elemenata |
| `doOnNext(Consumer<T>)` | pre slanja svake vrednosti dalje |
| `doOnComplete(Runnable)` | (Flux) kad tok normalno završi |
| `doOnSuccess(Consumer<T>)` | (Mono) kad tok normalno završi |
| `doOnError(Consumer<Throwable>)` | kad tok pukne |
| `doOnTerminate(Runnable)` | onComplete *ili* onError (ne na cancel) |
| `doFinally(Consumer<SignalType>)` | uvek - i na cancel |
| `doOnCancel(Runnable)` | kad subscriber otkaže |

```java
Flux.range(1, 3)
    .doOnSubscribe(sub  -> System.out.println("[hook] subscribe"))
    .doOnRequest(n     -> System.out.println("[hook] request(" + n + ")"))
    .doOnNext(v        -> System.out.println("[hook] next: " + v))
    .doOnComplete(()   -> System.out.println("[hook] complete"))
    .doFinally(sig     -> System.out.println("[hook] finally: " + sig))
    .subscribe();
```

Tipičan ispis:

```
[hook] subscribe
[hook] request(9223372036854775807)
[hook] next: 1
[hook] next: 2
[hook] next: 3
[hook] complete
[hook] finally: onComplete
```

> **Pravilo iz week 1, ponovljeno:** side-effects pripadaju *isključivo* u
> `doOn*` hooks. **Nikad** u `map`. `map` mora biti čista funkcija; ako
> dođe do `retry`, ceo `map` se ponovo izvršava i side-effect se *ponovi*.

### 4.3. `log()` - najbolji prijatelj kad nešto ne radi

`log()` operator ispisuje *sve* signale dok prolaze, sa imenom kategorije
i thread-om. Idealan za debug - ne moramo ručno da pišemo svaki `doOn*`.

```java
Flux.range(1, 3)
    .map(n -> n * 10)
    .log("posle.map")
    .filter(n -> n > 10)
    .log("posle.filter")
    .subscribe();
```

Ispisuje:

```
[posle.map]    onSubscribe(...)
[posle.map]    request(unbounded)
[posle.map]    onNext(10)
[posle.filter] onSubscribe(...)
[posle.filter] request(unbounded)
[posle.filter] onNext(20)   ← 10 nije prošao filter
[posle.filter] onNext(30)
[posle.map]    onComplete()
[posle.filter] onComplete()
```

Vidimo *tačno* gde i šta se dešava. Bez `log()`, debug reaktivnih
tokova je sporiji nego što treba.

> **Demo:** [`LifecycleSignals.java`](LifecycleSignals.java)

---

## 5. Osnovni operatori - preklapanje sa `Stream`-om

Većina operatora koje ste naučili u prvoj polovini semestra na `Stream<T>`
**postoje** i na `Flux<T>` / `Mono<T>` sa *istim* značenjem.

| Operator | `Stream<T>` | `Flux<T>` / `Mono<T>` | Napomena |
|----------|-------------|------------------------|----------|
| `map(Function)` | da | da | po-element transformacija |
| `filter(Predicate)` | da | da | po-element filter |
| `take(n)` / `limit(n)` | da (`limit`) | da (`take`) | uzmi prvih n |
| `skip(n)` | da | da | preskoči prvih n |
| `distinct()` | da | da | bez dupliciranih |
| `distinctUntilChanged()` | ne | da | dupliciran *uzastopno* |
| `count()` | da | `Mono<Long>` | broj elemenata |
| `reduce(BinaryOperator)` | da | `Mono<T>` | redukcija u jednu vrednost |
| `reduce(seed, BiFunction)` | da | `Mono<R>` | sa početnom vrednošću |
| `scan(BinaryOperator)` | ne (samo `reduce`) | `Flux<T>` | redukcija koja emituje *međurezultate* |
| `collect(Collector)` | da | `Mono<R>` | u kolekciju |
| `collectList()` | da (`toList()`) | `Mono<List<T>>` | čest slučaj |
| `collectMap(keyFn)` | preko `Collectors.toMap` | `Mono<Map<K,T>>` | |
| `any(Predicate)` / `all(...)` | `anyMatch`, `allMatch` | `Mono<Boolean>` | |
| `sort()` / `sort(Comparator)` | da | da | (potreban je terminalan tok!) |

```java
// Stream - primer
List<String> rezultat = List.of("ana", "marko", "petar").stream()
    .filter(s -> s.length() > 3)
    .map(String::toUpperCase)
    .toList();   // [MARKO, PETAR]

// Flux - IDENTIČAN pipeline, samo asinhron
Mono<List<String>> rezultatM = Flux.just("ana", "marko", "petar")
    .filter(s -> s.length() > 3)
    .map(String::toUpperCase)
    .collectList();   // Mono<[MARKO, PETAR]>
```

### 5.1. `reduce` vs. `scan` - mala ali važna razlika

Ovo je sitnica koja postoji u Reactor-u, a u `Stream` API-ju ne. `reduce`
emituje *jednu* vrednost na kraju; `scan` emituje *svaki međurezultat*.

```java
Flux.range(1, 5)
    .reduce(0, Integer::sum)
    .subscribe(System.out::println);
// 15  (samo finalna suma)

Flux.range(1, 5)
    .scan(0, Integer::sum)
    .subscribe(System.out::println);
// 0, 1, 3, 6, 10, 15  (kumulativna suma na svakom koraku)
```

`scan` je koristan za "running total"-e - npr. tekuća suma transakcija,
tekući broj korisnika, tekući prosek.

> **Demo:** [`OsnovniOperatori.java`](OsnovniOperatori.java)

---

## 6. Operatori kojih nema u `Stream`-u (vremenski, fallback)

Stream API ne zna ništa o vremenu - sve se "desi sad". Reactor-u je
vreme prvoklasni pojam; tu su operatori koje na `Stream`-u jednostavno
ne možemo imati.

### 6.1. Vremenski operatori

```java
// Razmak između elemenata (tok pratimo kroz vreme)
Flux.range(1, 5)
    .delayElements(Duration.ofMillis(300))   // svaki element 300ms posle prethodnog
    .subscribe(System.out::println);

// Kasni početak (subscribe se "okida" tek kasnije)
Flux.just("kasno")
    .delaySubscription(Duration.ofSeconds(1))
    .subscribe(System.out::println);

// Timeout - ako se ne emituje za dato vreme, padne sa TimeoutException
Mono.just("brzo")
    .delayElement(Duration.ofSeconds(2))
    .timeout(Duration.ofSeconds(1))
    .subscribe(
        v   -> System.out.println("dobio: " + v),
        err -> System.err.println("timeout: " + err));
```

`take` i `skip` imaju i *vremenske* varijante:

```java
Flux.interval(Duration.ofMillis(100))
    .take(Duration.ofSeconds(1))             // uzmi koliko stigne za 1 sekundu (oko 10)
    .subscribe(System.out::println);

Flux.interval(Duration.ofMillis(100))
    .skip(Duration.ofMillis(500))            // ignorisi prvih 500ms
    .take(5)
    .subscribe(System.out::println);
```

### 6.2. Fallback i prazni-tok operatori

`Stream` ne razdvaja "prazno" od "ima jedan element"; reaktivni tok da.
Reactor ima dedicated operatore za to:

```java
// Ako tok završi prazan, vrati zadatu default vrednost.
Flux.<String>empty()
    .defaultIfEmpty("nema podataka")
    .subscribe(System.out::println);   // "nema podataka"

// Ako tok završi prazan, prebaci se na drugi izvor.
Mono.<User>empty()
    .switchIfEmpty(Mono.fromCallable(() -> ucitajIzKesa()))
    .subscribe();
```

`switchIfEmpty` je posebno koristan za *fallback chain*:

```java
nadjiUKesu()
    .switchIfEmpty(nadjiUBazi())
    .switchIfEmpty(nadjiPrekoApija())
    .subscribe();
```

### 6.3. Repeat - ponovi tok

```java
// Ponovi izvor 3 puta (ukupno 4 emisije: original + 3 ponavljanja)
Flux.just("ping")
    .repeat(3)
    .subscribe(System.out::println);   // ping, ping, ping, ping
```

`repeat` se okida na **onComplete** signalu. Ako tok pukne, repeat
ne radi - za to služi `retry` (week 5).

### 6.4. Kasnije

`onErrorReturn`, `onErrorResume`, `retry`, `retryWhen` - sve je deo
*error handling*-a, koji ćemo gledati kasnije. Spomenute samo da znate da postoje.

> **Demo:** [`VremenskiOperatori.java`](VremenskiOperatori.java)

---

## 7. `block()` - kad i zašto u demo kodu

Ceo poenta reaktivnog modela je *neblokirajuće* izvršavanje. Pa zašto
onda u našem demo kodu skoro stalno vidimo `.block()` ili `.blockLast()`
na kraju?

**Odgovor:** zato što `main` metoda nije reaktivni kontekst.

```java
public static void main(String[] args) {
    Flux.interval(Duration.ofMillis(100))
        .take(5)
        .subscribe(System.out::println);
    // main() ovde završi pre nego što se prvi tick desi.
    // JVM gasi sve daemon niti - ništa se ne ispiše.
}
```

`.subscribe()` je **non-blocking** - vraća se *odmah*, a tok teče u
pozadini na drugoj niti. Ako `main` ne čeka, JVM se ugasi.

`block`/`blockLast`/`blockFirst` čekaju do završetka:

```java
public static void main(String[] args) {
    Flux.interval(Duration.ofMillis(100))
        .take(5)
        .doOnNext(System.out::println)
        .blockLast();   // <- main čeka dok poslednji element ne stigne
}
```

### 7.1. Varijante

| Metod | Tip | Vraća | Kad nema vrednosti |
|-------|-----|-------|--------------------|
| `Mono.block()` | Mono | `T` | `null` (ili `IllegalStateException` ako tok pukne) |
| `Mono.blockOptional()` | Mono | `Optional<T>` | `Optional.empty()` |
| `Flux.blockFirst()` | Flux | `T` | `null` |
| `Flux.blockLast()` | Flux | `T` | `null` |
| `Mono.block(Duration)` / `Flux.blockLast(Duration)` | both | T ili throw `IllegalStateException` | timeout |

### 7.2. Kad `block()` *jeste* OK

- demo kod, glavni `main` u školskom primeru;
- testovi koji *moraju* da provere finalnu vrednost (mada `StepVerifier`
  je bolji izbor);
- CLI alati gde je sinhrono čekanje *cilj*.

### 7.3. Kad `block()` *nije* OK - "incident pricelist"

> **Incident:** Production server u jednom Spring WebFlux projektu se
> "zaledio" pod opterećenjem. Zatekli smo `.block()` u jednom servisnom
> sloju, pozvanom iz event-loop niti. Event loop je nit koja servisira
> *hiljade* paralelnih HTTP konekcija; kad je blokirana, *sve* konekcije
> stoje. Lek: nikad `block` u handler-skom kodu. `flatMap`,
> `subscribeOn` su tu da nikad ne moramo da blokiramo.

**Pravilo:** `block` je dozvoljen *samo* na granici sa neredaktivnim
svetom (main, test, CLI). Unutar reaktivnog pipeline-a - nikad.

> **Demo:** [`BlockingDemo.java`](BlockingDemo.java)

---

## 8. Šta dolazi sledeće nedelje

Sad imamo izvore i osnovne operatore. Ono što sledeće nedelje radimo -
**kombinovanje više tokova** i **asinhrona transformacija**:

| Operator | Šta radi | Zašto je važan |
|----------|----------|----------------|
| `flatMap(T -> Mono/Flux)` | za svaki element pokreni novi async tok i *spoji* rezultate | osnova svake reaktivne kompozicije |
| `concatMap(T -> Mono/Flux)` | kao `flatMap`, ali *garantuje redosled* - radi serijski | kad redosled važi (npr. transakcije) |
| `switchMap(T -> Mono/Flux)` | "preklopi" na novi tok - otkaži stari kad stigne novi | za autocomplete, search-as-you-type |
| `merge(F1, F2, ...)` | spoji više Flux-eva u jedan, *paralelno* | event tokovi različitog porekla |
| `concat(F1, F2, ...)` | spoji *redom* - F2 počinje tek kad F1 završi | kompozicija sa redosledom |
| `zip(F1, F2, ...)` | spoji *po jedan* iz svakog izvora u tuple | čekaš na *sve* nezavisne pozive |
| `combineLatest(...)` | uvek emituj sa najnovijim iz svakog izvora | UI state koji zavisi od više izvora |

---

## 9. Primeri koda i vežbe

| Fajl | Tema |
|------|------|
| [`MonoVsFluxDemo.java`](MonoVsFluxDemo.java) | Kada Mono, kada Flux, konverzije |
| [`KreiranjeIzvora.java`](KreiranjeIzvora.java) | Sve glavne factory metode (`just`, `range`, `defer`, `generate`, `create`, ...) |
| [`LifecycleSignals.java`](LifecycleSignals.java) | `subscribe` varijante, `doOn*` hooks, `log()` |
| [`OsnovniOperatori.java`](OsnovniOperatori.java) | `map` / `filter` / `take` / `distinct` / `reduce` / `scan` / `collectList` |
| [`VremenskiOperatori.java`](VremenskiOperatori.java) | `delayElements` / `timeout` / `take(Duration)` / `defaultIfEmpty` / `switchIfEmpty` / `repeat` |
| [`BlockingDemo.java`](BlockingDemo.java) | Kad `block` / `blockFirst` / `blockLast` / `blockOptional`, kad NE |
| [`PracticeTasksForStudents.java`](PracticeTasksForStudents.java) | Zadaci za samostalnu vežbu |
| [`PracticeTasksSolutions.java`](PracticeTasksSolutions.java) | Rešenja zadataka |

---
