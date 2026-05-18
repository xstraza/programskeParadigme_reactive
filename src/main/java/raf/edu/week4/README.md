# Paradigme Programiranja - Nedelja 4
## Schedulers, paralelizam i tajming

> Do sada smo komponovali tokove **bez razmišljanja o nitima**. Sve je
> "nekako radilo" - `delayElements` je sam pokretao nit, `flatMap` je
> davao paralelizam, itd. Ova nedelja sve to čini **eksplicitnim**:
> kontrolišemo gde se rad izvršava i koliko paralelizma dozvoljavamo.

---

## Sadržaj

1. [Uvod - zašto treba o nitima](#1-uvod--zašto-treba-o-nitima)
2. [Schedulers - vrste i kada koju](#2-schedulers--vrste-i-kada-koju)
3. [`subscribeOn` - nit za upstream](#3-subscribeon--nit-za-upstream)
4. [`publishOn` - nit za downstream](#4-publishon--nit-za-downstream)
5. [Praktični obrasci](#5-praktični-obrasci)
6. [`Flux.parallel()` - eksplicitni paralelizam](#6-fluxparallel--eksplicitni-paralelizam)
7. [Brza referenca](#7-brza-referenca)
8. [Šta dolazi sledeće nedelje](#8-šta-dolazi-sledeće-nedelje)
9. [Primeri koda i vežbe](#9-primeri-koda-i-vežbe)

---

## 1. Uvod

Reactor je **konkurentno agnostičan** - operatori po default-u rade na
**istoj niti** na kojoj je subscription nastao. Ako ništa eksplicitno
ne kažemo, ceo `Flux` se izvršava na niti `main`.

```java
Flux.range(1, 3)
    .map(i -> i * 10)
    .filter(i -> i > 10)
    .subscribe(i -> System.out.println(Thread.currentThread().getName() + ": " + i));
// main: 20
// main: 30
```

Sve je sinhrono dok ne uvedemo nešto što **prebacuje nit**:

- vremenski operator (`delayElements`, `interval`) - interno koristi
  `Schedulers.parallel`,
- async izvor (`Mono.fromFuture`, `Flux.create` sa drugom niti),
- eksplicitan `subscribeOn` / `publishOn`,
- `flatMap` čiji unutrašnji `Publisher` već radi na drugoj niti.

**Pravilo broj 1:** `Publisher` ne radi ništa dok se ne pretplatiš.
**Pravilo broj 2:** nit subscribe-a određuje gde rad počinje - sve dok
neki operator to ne promeni.

> `Stream` u Javi nije imoe koncept niti -
> `.parallel()` postoji, ali je crna kutija. U reaktivnom modelu **nit
> je eksplicitan parametar** svakog operatora koji ima vremensku
> dimenziju. To nas tera da razmislimo: blokira li ovaj poziv? Da li smemo
> ovo da pustimo na CPU pool? Reaktivni model kažnjava nepažnju ka
> nitima - ali zauzvrat daje kontrolu.

### Šta se dešava kad blokiramo na pogrešnoj niti

```java
// LOŠE - Schedulers.parallel ima fiksan broj niti = broj jezgara.
// JDBC poziv blokira nit. Već za 4-8 paralelnih poziva poolovi su
// zauzeti i ceo sistem stoji.
Flux.range(1, 100)
    .flatMap(id -> Mono.fromCallable(() -> jdbc.findById(id)))   // blokira!
    .subscribe();
```

Rešenje je `Schedulers.boundedElastic` (videti dole) - ima mnogo niti
i pravljen je upravo za blocking pozive.

### Gotcha: vremenski operatori implicitno koriste `parallel`

`delayElements`, `interval`, `timeout` - sve što već znamo iz nedelje 2 -
**ne radi na niti subscribe-a**. Interno koriste `Schedulers.parallel`,
i to je razlog što odjednom vidimo `parallel-3` u logu iako nismo
ništa eksplicitno tražili.

```java
Flux.range(1, 3)
    .doOnNext(n -> log("pre", n))            // main
    .delayElements(Duration.ofMillis(100))
    .doOnNext(n -> log("post", n))           // parallel-X !
    .blockLast();
```

Praktične posledice:
- ne smemo posle `delayElements`-a da nastavimo sa blocking pozivom bez
  `publishOn(boundedElastic)` - već smo na CPU pool-u,
- svaki vremenski operator prima opcioni `Scheduler` parametar
  (`delayElements(d, scheduler)`) - ako ne želimo `parallel`, prosledimo
  drugi.

---

## 2. Schedulers - vrste i kada koju

`Scheduler` je apstrakcija nad pool-om niti. Reactor isporučuje četiri
gotova preko `reactor.core.scheduler.Schedulers`:

| Scheduler | Broj niti | Pravljen za                                   | Tipičan slučaj |
|-----------|-----------|-----------------------------------------------|----------------|
| `parallel()` | `Runtime.availableProcessors()` (fiksno) | CPU rad, ne-blokirajući kod                   | `map` heavy compute, parsing, kratke transformacije |
| `boundedElastic()` | rastući do `10 × cores` (cap), TTL 60s | **blocking I/O** (JDBC, fajl, legacy klijent) | `Mono.fromCallable` koji zove JDBC |
| `single()` | 1 (single-thread) | serijski rad gde je redosled bitan            | log writer, single connection |
| `immediate()` | nema niti - radi na trenutnoj | testovi, opt-out iz scheduler-a               | retko u produkciji |

```java
import reactor.core.scheduler.Schedulers;

Scheduler cpu  = Schedulers.parallel();
Scheduler io   = Schedulers.boundedElastic();
Scheduler one  = Schedulers.single();
```

### Zašto baš četiri

Različite vrste posla imaju **drugačiji profil**:

- **CPU rad** je kratak i ne blokira - par niti dovoljno (više ne pomaže,
  context switch postaje overhead).
- **Blocking I/O** spava - možemo ih imati puno, niti same po
  sebi ne troše CPU.
- **Single-thread** garantuje redosled bez sinhronizacije.

`parallel()` se NE sme koristiti za blocking - pool je mali, lako
"jede" sam sebe. Reactor čak ima `BlockHound` library koji u testu
**baca exception** ako otkrije `Thread.sleep` ili JDBC poziv na
`parallel` niti.

### Kako prepoznati blocking poziv

Ako poziv:
- otvara TCP/HTTP konekciju (`URLConnection`, `HttpClient.send`
  sinhrono, `Socket`),
- čita iz baze preko JDBC-a (`Connection.prepareStatement.executeQuery`),
- čita/upisuje fajl pomoću `java.io` (`FileInputStream.read`),
- poziva legacy SDK (Stripe, AWS sdk verzija 1, itd.),

- **blokira**. Zaobiđi sa `Schedulers.boundedElastic`.

> **Demo:** [`SchedulerTypesDemo.java`](SchedulerTypesDemo.java)

---

## 3. `subscribeOn` - nit za upstream

`subscribeOn(scheduler)` kaže: **subscription se desi na ovom
scheduler-u**. To utiče na ceo lanac **iznad** sebe (upstream), uključujući
sam izvor.

```java
Flux.range(1, 3)
    .map(i -> {
        log("map", i);
        return i * 10;
    })
    .subscribeOn(Schedulers.parallel())
    .subscribe(i -> log("subscribe", i));

//  [parallel-1] map        -> 1
//  [parallel-1] subscribe  -> 10
//  [parallel-1] map        -> 2
//  ...
```

Sve se izvršava na `parallel-1` - i izvor (`range`), i `map`, i
`subscribe` callback.

### Ključne osobine

- **Samo jedan `subscribeOn` ima efekta** - onaj **najbliži izvoru**.
  Drugi `subscribeOn` niže u lancu se ignoriše.
- Mesto u lancu **ne** menja ponašanje:
  ```java
  src.map(...).filter(...).subscribeOn(io)     // izvor i sve radi na io
  src.subscribeOn(io).map(...).filter(...)     // isto
  ```
- Tipičan slučaj: imamo blocking izvor (`Mono.fromCallable(jdbc::query)`),
  i hoćemo da subscription predje sa `main` niti na `boundedElastic`.

### Anti-pattern

```java
// LOŠE - subscribeOn na blocking pozivu, ali na blokirajućoj niti
blockingMono.subscribeOn(Schedulers.parallel());

// DOBRO
blockingMono.subscribeOn(Schedulers.boundedElastic());
```

Tipovi scheduler-a nisu interchangeable - pravilan izbor zavisi od
toga **šta unutra radi**.

---

## 4. `publishOn` - nit za downstream

`publishOn(scheduler)` kaže: **od ove tačke nadole, idi na ovaj
scheduler**. Sve **iznad** ostaje gde je bilo.

```java
Flux.range(1, 3)
    .map(i -> {
        log("map-pre", i);          // nit prethodnog dela
        return i * 10;
    })
    .publishOn(Schedulers.parallel())
    .map(i -> {
        log("map-post", i);         // parallel-X
        return i + 1;
    })
    .subscribe(i -> log("subscribe", i));   // parallel-X
```

### Više `publishOn`-ova

Za razliku od `subscribeOn`-a, **`publishOn` MOŽE da se nadovezuje**.
Svaki menja nit za sledeći segment lanca:

```java
flux
    .publishOn(Schedulers.boundedElastic())   // segment A
    .map(this::blockingParseFromString)
    .publishOn(Schedulers.parallel())          // segment B
    .map(this::cpuHeavyTransform)
    .subscribe();
```

Ovo je **najmoćniji pattern u Reactor-u**: blocking deo prebacimo na
elastic pool, CPU deo na parallel, finalni subscribe gde god nam
odgovara.

### `subscribeOn` vs `publishOn` - jedna slika

```
src ──map──filter──publishOn(A)──map──publishOn(B)──map──subscribe
└────upstream─────┘             └──A──┘             └──B──┘
└────  subscribeOn ovde važi za sve do prvog publishOn ──┘
```

| | `subscribeOn`                 | `publishOn` |
|---|-------------------------------|---|
| Šta menja | Nit subscription-a (upstream) | Nit od te tačke (downstream) |
| Više instanci | Najbliži izvoru pobeđuje      | Sve važe, lanac može da menja nit |
| Mesto u lancu | Nebitno*                      | Bitno |
| Tipičan slučaj | Blocking izvor van `main`     | Razdvajanje I/O i CPU faza |

> **Demo:** [`SubscribeOnVsPublishOnDemo.java`](SubscribeOnVsPublishOnDemo.java)

---

## 5. Praktični obrasci

### Obrazac 1: blocking poziv u reaktivnom toku

```java
Mono<User> findUser(long id) {
    return Mono.fromCallable(() -> jdbc.findUserById(id))
            .subscribeOn(Schedulers.boundedElastic());   // blocking → elastic
}
```

Pravilo: **svaki `Mono.fromCallable` koji unutra radi blocking pozive
mora imati `subscribeOn(boundedElastic)`**.

### Obrazac 2: CPU heavy rad u sredini lanca

```java
flux
    .publishOn(Schedulers.parallel())           // CPU pool
    .map(this::parsePdfPage)                    // CPU heavy
    .map(this::extractText)
    .publishOn(Schedulers.boundedElastic())     // nazad na I/O pool
    .flatMap(this::saveToDb)                    // blocking JDBC
    .subscribe();
```

`publishOn` je tu "thread-switcher". Ne moramo da uvodimo `subscribeOn` -
ako izvor već radi na nekom poolu, samo prebacimo dalje.

### Obrazac 3: kombinovanje async izvora

```java
Mono.zip(
        Mono.fromCallable(() -> jdbc.findUser(1)).subscribeOn(io),
        Mono.fromCallable(() -> jdbc.findOrder(1)).subscribeOn(io),
        Mono.fromCallable(() -> jdbc.findInvoice(1)).subscribeOn(io))
    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()))
    .subscribe();
```

Sva tri `Mono`-a izvršavaju se **paralelno** jer svaki ima svoj
`subscribeOn` na elastic. `zip` samo skuplja rezultate.

### Obrazac 4: polling preko `interval`-a

`Flux.interval(Duration)` smo videli u nedelji 2 kao beskonačni tajmer.
Pravu vrednost dobija kad ga spojimo sa `flatMap`-om i blocking pozivom
- to je standardni **polling pattern**:

```java
Flux.interval(Duration.ofSeconds(5))
    .flatMap(tick -> Mono.fromCallable(() -> api.fetchStatus())
                         .subscribeOn(Schedulers.boundedElastic()))
    .subscribe(status -> ui.refresh(status));
```

Svakih 5s okida fetch, fetch ide na elastic pool (jer je blocking),
rezultat ide u UI. Ako želimo strogo **serijski** polling (sledeći
poll tek kad prethodni završi), koristimo `concatMap` umesto
`flatMap`-a.

> ⚠️ `interval` je beskonačan. Bez `take`/`takeUntil`/`dispose()`
> radi zauvek. U produkciji se obično vezuje za lifecycle (npr.
> dispose pri zatvaranju komponente).

### Obrazac 5: timeout sa fallback Publisher-om

`timeout(Duration)` iz nedelje 2 baca `TimeoutException`. Postoji i
varijanta sa **rezervnim Publisher-om**:

```java
brzApi.fetch(id)
    .timeout(Duration.ofMillis(300), cacheBackup.fetch(id))
    .subscribe(this::prikazi);
```

Ako primarni izvor ne stigne za 300ms, automatski se prebacuje na
fallback - bez exception-a, bez `onErrorResume`-a. Klasičan slučaj:
brz upstream sa kratkim timeout-om, sporiji ali pouzdaniji cache kao
backup.

U nedelji 5 ćemo videti kako se ovo kombinuje sa `retry` i
`Retry.backoff`-om u kompletnu strategiju otpornosti.

> **Demo:** [`BlockingWrapperDemo.java`](BlockingWrapperDemo.java)

---

## 6. `Flux.parallel()` - eksplicitni paralelizam

`flatMap` daje paralelizam (do 256 unutrašnjih tokova), ali ne **na
više niti** sam po sebi - sve radi na niti subscription-a osim ako
unutrašnji tok ne prebaci nit.

Za **stvarno paralelan CPU rad** preko više jezgara, postoji
`Flux.parallel()`:

```java
Flux.range(1, 100)
    .parallel(4)                              // 4 "rail"-a
    .runOn(Schedulers.parallel())             // svaki rail na CPU poolu
    .map(this::cpuHeavyTransform)             // izvršava se na 4 niti istovremeno
    .sequential()                             // vraćamo se u običan Flux
    .subscribe(System.out::println);
```

### Šta se zapravo desi

1. `parallel(N)` deli tok na **N rail-ova** (round-robin po default-u).
2. `runOn(scheduler)` zakači svaki rail na zaseban worker scheduler-a.
3. Operatori posle (`map`, `filter`, ...) izvršavaju se **paralelno**
   na svakom rail-u.
4. `sequential()` merge-uje rail-ove nazad u jedan `Flux`. Bez nje
   imamo `ParallelFlux`, ne `Flux`.

### Kada `parallel()` umesto `flatMap`-a

- `flatMap` je za **async izvore** - svaki element okida nezavisni
  `Publisher` koji "sam zna" gde radi.
- `parallel().runOn()` je za **CPU rad na sinhronoj transformaciji**
  koju hoćemo na više jezgara.

```java
// flatMap pattern (async izvori)
flux.flatMap(id -> httpKlijent.get(id))

// parallel pattern (CPU rad)
flux.parallel().runOn(Schedulers.parallel()).map(this::compute).sequential()
```

> ⚠️ Ne mešati ih: `flux.parallel().runOn(io).map(blockingCall)` deluje
> kao da radi - radi, ali krenemo li sa stotinama elemenata, blocking
> pozivi će nam pojesti niti i performance je **gora** nego sa
> `flatMap + subscribeOn(io)`.

> **Demo:** [`ParallelFluxDemo.java`](ParallelFluxDemo.java)

---

## 7. Brza referenca

### "Imam blocking poziv u Mono/Flux-u"

```java
Mono.fromCallable(() -> jdbc.find(id))
    .subscribeOn(Schedulers.boundedElastic());
```

### "Hoću da prebacim downstream na CPU pool"

```java
flux.publishOn(Schedulers.parallel())
    .map(this::cpuHeavy);
```

### "Hoću periodičan poll svake N sekunde"

```java
Flux.interval(Duration.ofSeconds(N))
    .flatMap(tick -> api.fetch())
    .subscribe();
```

### "Ovaj poziv ne sme da traje više od 3s"

```java
mono.timeout(Duration.ofSeconds(3))
    .onErrorReturn(default);
```

### "Hoću da paralelizujem CPU heavy transformaciju"

```java
flux.parallel(4)
    .runOn(Schedulers.parallel())
    .map(this::compute)
    .sequential();
```

### Scheduler - koji za šta

| Šta radim | Scheduler |
|-----------|-----------|
| `Thread.sleep`, JDBC, fajl I/O, legacy SDK | `boundedElastic` |
| `map` koji parsuje / računa | `parallel` |
| Serijski log writer, single connection | `single` |
| Test / debug bez nit-prebacivanja | `immediate` |

### `subscribeOn` vs `publishOn` - spickli

| Pitanje | `subscribeOn` | `publishOn` |
|---------|---------------|-------------|
| Utiče na koga | Subscription (upstream + sve dok ga `publishOn` ne pregazi) | Sve niže od pozicije |
| Mesto u lancu | Nebitno | Bitno |
| Više instanci | Samo prva važi | Sve važe |

---

## 8. Šta dolazi sledeće nedelje

Sa schedulers-ima i tajmingom pod kontrolom, sledeća nedelja se bavi
**lošim scenarijima** - šta kad producer brže emituje nego što consumer
može da obradi, i šta kad pozivi padaju.

| Tema | Operator |
|------|----------|
| Backpressure strategije | `onBackpressureBuffer/Drop/Latest/Error`, `limitRate` |
| Error handling | `onErrorReturn`, `onErrorResume`, `doOnError` |
| Retry | `retry(n)`, `Retry.backoff(...)`, jitter |
| Praktični pattern | `WebClient` sa timeout + retry + fallback |

Sve to nadograđuje znanje iz ove nedelje: backoff koristi scheduler,
retry diže ceo lanac od nule (i ako si imao `subscribeOn`, on se
ponovo aktivira), timeout već znamo.

---

## 9. Primeri koda i vežbe

| Fajl | Tema |
|------|------|
| [`SchedulerTypesDemo.java`](SchedulerTypesDemo.java) | `parallel`, `boundedElastic`, `single`, `immediate` - imena niti i ponašanje |
| [`SubscribeOnVsPublishOnDemo.java`](SubscribeOnVsPublishOnDemo.java) | razlika između dva operatora, kombinacija u jednom lancu |
| [`BlockingWrapperDemo.java`](BlockingWrapperDemo.java) | obrazac za blocking pozive, polling preko `interval`-a, `timeout` sa fallback-om |
| [`ParallelFluxDemo.java`](ParallelFluxDemo.java) | `parallel().runOn(...).sequential()`, CPU rad na više jezgara |
| [`PracticeTasksForStudents.java`](PracticeTasksForStudents.java) | Zadaci za samostalnu vežbu |
| [`PracticeTasksSolutions.java`](PracticeTasksSolutions.java) | Rešenja zadataka |

---

## Reference

- Reactor Reference Guide - [Threading and Schedulers](https://projectreactor.io/docs/core/release/reference/#schedulers)
- Reactor Reference Guide - [`publishOn` vs `subscribeOn`](https://projectreactor.io/docs/core/release/reference/#_the_publishon_method)
- BlockHound - [github.com/reactor/BlockHound](https://github.com/reactor/BlockHound) (detekcija blocking poziva u runtime-u)

---
