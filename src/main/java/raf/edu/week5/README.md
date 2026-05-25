# Paradigme Programiranja - Nedelja 5
## Backpressure, error handling i retry

> Do sada smo pretpostavljali da pipeline "samo radi" - producer emituje,
> consumer trošٕi, niko se ne zbunjuje. Ova nedelja pokriva **dva
> realna scenarija** koji to ruše: (1) producer emituje **brže** nego što
> consumer može da obradi, (2) negde u lancu **nešto pukne**. Reactor
> ima eksplicitne mehanizme za oba - backpressure strategije za prvi,
> `onErrorXxx` / `retry` familija za drugi.

---

## Sadržaj

1. [Uvod - šta je backpressure](#1-uvod--šta-je-backpressure)
2. [Backpressure strategije](#2-backpressure-strategije)
3. [`limitRate` - kontrolisan request](#3-limitrate--kontrolisan-request)
4. [Error handling - signal `onError`](#4-error-handling--signal-onerror)
5. [`onErrorReturn` / `onErrorResume` / `onErrorContinue`](#5-onerrorreturn--onerrorresume--onerrorcontinue)
6. [Retry - `retry(n)`, `retryWhen`, `Retry.backoff`](#6-retry--retryn-retrywhen-retrybackoff)
7. [Praktični obrasci](#7-praktični-obrasci)
8. [Brza referenca](#8-brza-referenca)
9. [Šta dolazi sledeće nedelje](#9-šta-dolazi-sledeće-nedelje)
10. [Primeri koda i vežbe](#10-primeri-koda-i-vežbe)

---

## 1. Uvod - šta je backpressure

**Backpressure** je mehanizam kojim consumer kaže producer-u: *"stani,
ne mogu da pratim"*. U Reactive Streams specifikaciji to ide preko
`Subscription.request(n)` - subscriber eksplicitno traži `n` elemenata,
producer ne sme da pošalje više od toga.

```
Subscriber           Publisher
    │── request(10) ──>│
    │<── onNext(x1) ───│
    │<── onNext(x2) ───│
    │      ...         │
    │<── onNext(x10) ──│
    │── request(10) ──>│   (sledeća tura)
```

Većinu vremena ovo se dešava **automatski** - operatori imaju default
strategiju (`request(Long.MAX_VALUE)` ili sa malim prefetch-om).
Backpressure postaje **problem** kad:

- producer je brži od consumer-a (npr. `Flux.interval(1ms)` + spor
  `subscribe`),
- izvor je **hot** - emituje bez obzira da li ga neko sluša (klikovi
  miša, network paketi, broker poruke),
- imamo bounded buffer i ne smemo da pojedemo memoriju.

> **Podsećanje:** kod `Stream`-a u Javi consumer **vuče** (pull) -
> `forEach` traži sledeću vrednost, `Stream` je sinhron. Kod reaktivnog
> modela producer **gura** (push) - i zato nam treba protokol za "stani
> malo". Backpressure je taj protokol.

### Cold vs hot - zašto je hot opasniji

- **Cold** izvor (`Flux.range`, `Mono.fromCallable`) pri svakom
  subscribe-u kreće od početka, **prirodno reaguje na request**.
  Backpressure je tu često nevidljiv jer producer svakako čeka.
- **Hot** izvor (`Sinks`, klikovi, brokerske poruke) emituje **na
  svoju ruku** - šta ćemo sa elementom za koji nema request-a?
  Tu strategija postaje vidljiva: bacamo, čuvamo, ili rušimo tok.

### Kako prepoznati backpressure problem

- `OutOfMemoryError` na "nevinom" pipeline-u koji ima `Flux.interval` -
  default buffer-uje sve što ne stigne da obradi.
- `IllegalStateException: Could not emit X due to lack of requests` -
  ovo je signal da je producer hteo da emituje, a downstream nije imao
  kapacitet. Tipično na `Sinks`.
- Kašnjenje raste sa vremenom, throughput pada - producer puni interni
  buffer brže nego što ga consumer prazni.

---

## 2. Backpressure strategije

Kad producer nema pravo da uspori (npr. real-time događaji), Reactor
ima **četiri** strategije preko `onBackpressureXxx` operatora:

| Operator | Šta radi | Kada koristiti |
|----------|----------|----------------|
| `onBackpressureBuffer()` | čuva sve neisporučene elemente u memoriji | kratki burst-evi, znamo da je bound mali |
| `onBackpressureBuffer(N, OverflowStrategy)` | bounded buffer, pri popunjavanju primenjuje strategiju | produkcija - znamo limit |
| `onBackpressureDrop()` | baca **novi** element ako nema request-a | metrike, klikovi - gubitak je ok |
| `onBackpressureLatest()` | čuva **samo poslednji** element, baca starije | UI stanje, "važno je samo najnovije" |
| `onBackpressureError()` | baca `MissingBackpressureException` | striktan ugovor - ne sme da curi |

```java
Flux.interval(Duration.ofMillis(1))         // brzi producer
    .onBackpressureDrop(dropped ->
        log.warn("dropped {}", dropped))
    .publishOn(Schedulers.parallel(), /*prefetch=*/ 1)
    .map(this::slowProcess)                  // spor consumer
    .subscribe();
```

### Mentalni model

- **BUFFER** = "sačekaj me, sve mi je važno". Risk: OOM ako consumer
  nikad ne stigne.
- **DROP** = "preskoči ako si zauzet". Risk: gubitak podataka.
- **LATEST** = "kad si gotov, daj mi najsvežije stanje". Risk: gubitak
  istorije.
- **ERROR** = "nije moj problem, ja sam ugovor poštovao". Risk: stream
  umire.

Izbor strategije je **biznis odluka**, ne tehnička. Da li su pojedinačni
događaji bitni? Da li je samo trenutno stanje bitno? Da li možemo da
gubimo?

### Primer: razlika između DROP, LATEST i BUFFER

```java
// Producer: 100 elem/s, Consumer: 10 elem/s

// DROP    - vidimo: 0, 1, 2, ..., a posle: 0, 1, 2, 50, 51, ...
//           (skoči jer su 3-49 bačeni dok je consumer radio)

// LATEST  - vidimo: 0, 99, ...  (samo poslednje stanje između čitanja)

// BUFFER  - vidimo: 0, 1, 2, ..., 99, 100, ... bez gubitka,
//           ali memorija raste linearno sa zaostatkom
```

> **Demo:** [`BackpressureStrategiesDemo.java`](BackpressureStrategiesDemo.java)

---

## 3. `limitRate` - kontrolisan request

Druga strana medalje: umesto da nam producer nameće tempo, **mi
diktiramo** sa downstream-a koliko tražimo odjednom.

```java
fastFlux
    .limitRate(100)        // tražim po 100, ne više
    .flatMap(this::process)
    .subscribe();
```

`limitRate(N)` prevodi se na `request(N)` upstream-u, čeka da obradimo
75% (default `lowTide`), zatim traži još. Tako consumer drži
**konstantan inventory** - bez burst-a, bez gladi.

Razlika od `onBackpressure*`:

- `onBackpressure*` se vezuje za **izvor** - reaguje kad downstream ne
  može da prati.
- `limitRate` se vezuje za **consumer-a** - ograničava koliko vuče.

U praksi se često **kombinuju**: `limitRate` na consumer strani, a
`onBackpressureBuffer` na producer strani služi kao safety net.

---

## 4. Error handling - signal `onError`

`Publisher` ima tri terminalna signala: `onComplete`, `onError`,
`cancel`. Greška je **prvoklasan signal** - ne baca se sinhrono kao
`throw`, već se propagira kroz `onError(Throwable)`.

```
Flux: ── 1 ── 2 ── 3 ──X── (onError)
                         ▲
                         └── tok je MRTAV, neće biti više elemenata
```

**Ključna razlika od običnih izuzetaka:** kad jednom `onError` prođe,
tok je gotov. Ne možemo da "uhvatimo pa nastavimo dalje" - moramo
**preusmeriti** tok pre nego što greška stigne do `subscribe`-a.

### Tri pristupa - po efektu

1. **Zameni vrednost** (`onErrorReturn`) - "ako padne, daj default".
2. **Preusmerи tok** (`onErrorResume`) - "ako padne, pređi na drugi
   Publisher".
3. **Preskoči element i nastavi** (`onErrorContinue`) - "ako padne na
   ovom elementu, ignoriši ga i idi dalje".

### Šta NIJE error handling

```java
flux.subscribe(
    v  -> handle(v),
    err -> log.error("oops", err)     // ovo je samo LOG, tok je već mrtav
);
```

Drugi argument `subscribe`-a je **sink** za grešku, ne handler koji je
hvata. Tok već nije više aktivan kad se ovo izvrši - ne možemo da
"nastavimo". Stvarni handling se desi **pre** `subscribe`-a, na nekom
od operatora.

---

## 5. `onErrorReturn` / `onErrorResume` / `onErrorContinue`

### `onErrorReturn(fallback)`

Najjednostavnije - prevod greške u podrazumevanu vrednost. Tok se
**ipak završava** (jedan onNext sa fallback-om, pa onComplete), ali bez
greške.

```java
Mono.fromCallable(() -> riskyCall())
    .onErrorReturn("default")
    .subscribe(System.out::println);
```

Pratimo i tip greške:

```java
mono.onErrorReturn(TimeoutException.class, "timed-out")
    .onErrorReturn(IOException.class, "io-failed")
```

### `onErrorResume(fn)`

Vraća **novi Publisher** umesto pale grane. Moćnije od `Return`-a jer
fallback može i sam biti async.

```java
primary.fetch()
    .onErrorResume(ex -> {
        log.warn("primary failed: {}", ex.getMessage());
        return secondary.fetch();           // drugi Publisher
    })
```

Tipičan slučaj: **cascading fallback** - primary → cache → default:

```java
primary.fetch()
    .onErrorResume(ex -> cache.fetch())
    .onErrorReturn("none");
```

### `onErrorContinue(fn)`

Specijalan - **ignoriše grešku na pojedinačnom elementu** umesto da
ubije ceo tok. Korisno kod batch obrada gde nekoliko loših redova ne
sme da pokvari ostatak.

```java
Flux.range(1, 10)
    .map(n -> {
        if (n == 5) throw new RuntimeException("loš element");
        return n * 10;
    })
    .onErrorContinue((err, badItem) ->
        log.warn("preskačem {}: {}", badItem, err.getMessage()))
    .subscribe(System.out::println);

// 10, 20, 30, 40, 60, 70, 80, 90, 100  (5 preskočen, tok živi)
```

`onErrorContinue` ima **suptilnu semantiku** - utiče na **upstream**
operatore koji "razumeju" continue (`map`, `flatMap`). Ne svi operatori
ga poštuju, i pozicija u lancu je bitna. Tim Reactor-a savetuje
**eksplicitno hvatanje u `flatMap`** kao pouzdaniju alternativu:

```java
flux.flatMap(n -> Mono.fromCallable(() -> riskyOp(n))
                      .onErrorResume(ex -> Mono.empty()))   // tiho preskoči
```

### `onErrorMap` - preusmeri tip izuzetka

Ne zaustavlja tok - samo zamenjuje exception drugim. Korisno na
granicama slojeva (npr. SQL-Exception → DomainException):

```java
mono.onErrorMap(SQLException.class,
        ex -> new DataAccessException("DB error", ex))
```

### `doOnError` - side-effect, NE handling

Treba zapamtiti: `doOnError` **ne hvata** grešku - samo izvršava neki
side-effect (log, metrika) i pušta grešku dalje.

```java
mono.doOnError(ex -> log.error("call failed", ex))    // log
    .onErrorResume(ex -> fallback);                    // ovo zapravo hvata
```

> **Demo:** [`ErrorHandlingDemo.java`](ErrorHandlingDemo.java)

---

## 6. Retry - `retry(n)`, `retryWhen`, `Retry.backoff`

Retry znači: kad stigne `onError`, **ponovo se pretplati** na isti
Publisher. Tok kreće iznova - svi `subscribeOn`/`publishOn` se aktiviraju
ponovo, blocking pozivi se izvršavaju ponovo.

### `retry()` - beskonačno

```java
mono.retry();        // svaki put kad padne, pretplati se ponovo
```

**Beskonačno**. Bez backoff-a. U praksi se **skoro nikad** ne koristi
sam - producer može da padne odmah ponovo i napravimo hot loop.

### `retry(n)` - sa limitom

```java
mono.retry(3);       // do 3 pokušaja, posle se predaje (propagira onError)
```

Pokušaji su **odmah** - ako prvi padne iz timeout-a od 5s, drugi takođe
za 5s, treći isto. Sumarno 15s pre fail-a. Bez delay-a između.

### `retryWhen` + `Retry.backoff` - produkcijski pattern

`Retry.backoff(maxAttempts, firstBackoff)` daje **eksponencijalni
backoff** (1s, 2s, 4s, 8s, ...) sa **jitter**-om (random ±50%) da bismo
izbegli thundering herd.

```java
import reactor.util.retry.Retry;

mono.retryWhen(Retry.backoff(3, Duration.ofMillis(200))
        .jitter(0.5)
        .maxBackoff(Duration.ofSeconds(2)))
```

Šta se dešava:

- pokušaj 1: padne odmah
- čekaj ~200ms ± jitter
- pokušaj 2: padne
- čekaj ~400ms ± jitter
- pokušaj 3: padne
- čekaj ~800ms ± jitter (capped na maxBackoff)
- pokušaj 4: padne → propagira `Retry.RetryExhaustedException`

### `filter` - retry samo za određene greške

Po default-u retry pokriva **sve** exception-e. Često hoćemo retry samo
za prolazne (network, timeout), a `NullPointerException` ili
`IllegalArgumentException` da odmah padnu (bug u kodu).

```java
mono.retryWhen(Retry.backoff(3, Duration.ofMillis(200))
        .filter(ex -> ex instanceof IOException
                   || ex instanceof TimeoutException))
```

### `onRetryExhaustedThrow` - šta se vidi posle isteka

Po default-u `retryWhen` baca `RetryExhaustedException` koji obavija
poslednji uzrok. Možemo i da zadržimo originalni:

```java
Retry.backoff(3, Duration.ofMillis(200))
    .onRetryExhaustedThrow((spec, signal) -> signal.failure())
```

### `Retry.fixedDelay` - bez backoff-a

```java
Retry.fixedDelay(5, Duration.ofMillis(500))
```

Pet pokušaja, svaki 500ms posle prethodnog pada. Pravolinijski.
Korisno kad upstream ima poznat recovery time i ne treba eksponencijala.

### `Retry.indefinitely()` + filter

```java
Retry.indefinitely()
    .filter(ex -> ex instanceof TransientException)
```

Beskonačno, ali samo za određene greške - npr. WebSocket reconnect koji
mora da pokušava dok ima mreže.

> **Demo:** [`RetryDemo.java`](RetryDemo.java)

---

## 7. Praktični obrasci

### Obrazac 1: HTTP poziv sa timeout + retry + fallback

Klasik. Tri sloja zaštite kombinovana:

```java
httpKlijent.get(url)
    .timeout(Duration.ofSeconds(2))
    .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
            .filter(ex -> ex instanceof TimeoutException
                       || ex instanceof IOException))
    .onErrorResume(ex -> cachedValue());
```

Šta radi:
1. svaki pojedinačni poziv ima 2s timeout,
2. ako padne (timeout ili network), do 3 retry-a sa backoff-om,
3. ako i to padne (svi pokušaji potrošeni), pređemo na cache.

### Obrazac 2: brz producer, spor consumer (UI)

```java
clickStream                                            // hot, brz
    .onBackpressureLatest()                            // čuvamo samo zadnji
    .publishOn(Schedulers.boundedElastic(), 1)         // prefetch = 1
    .flatMap(click -> handleClick(click), 1)           // serijski
    .subscribe();
```

Najnoviji klik uvek nadjača prethodni - korisno kad obrada traje duže
od učestalosti klikova (npr. "save draft").

### Obrazac 3: batch sa skip-on-error

```java
Flux.fromIterable(rows)
    .flatMap(row -> processRow(row)
            .onErrorResume(ex -> {
                log.warn("loš red {}: {}", row.id, ex.getMessage());
                return Mono.empty();                   // preskoči
            }))
    .collectList();
```

Eksplicitan `onErrorResume` u `flatMap`-u je pouzdaniji od
`onErrorContinue` - granica greške je vidljiva, ne zavisi od operator
support-a.

### Obrazac 4: bounded buffer kao safety net

```java
hotSource
    .onBackpressureBuffer(
            /*maxSize=*/    1000,
            /*onOverflow=*/ dropped -> metrics.increment("dropped"),
            BufferOverflowStrategy.DROP_OLDEST)
    .publishOn(Schedulers.parallel())
    .subscribe(this::handle);
```

Maks 1000 zaostalih. Ako pređe - bacaj najstarije, beleži metriku.
Tok ne umire, memorija ne curi.

### Obrazac 5: idempotent retry sa side-effect-om

Ako side-effect (npr. POST request) **nije idempotent**, retry je
opasan - drugi pokušaj može da napravi duplikat. Rešenje:
**idempotent key** ili `onErrorResume` umesto retry-a.

```java
// LOŠE - duplicat POST na retry
api.createOrder(order).retry(3);

// DOBRO - server-side idempotency key
api.createOrder(order.withIdempotencyKey(UUID.randomUUID()))
   .retry(3);

// ILI - bez retry-a, jasan signal
api.createOrder(order)
   .onErrorResume(ex -> alertOps(order));
```

---

## 8. Brza referenca

### "Producer prebrz, ne sme da gubi podatke"

```java
flux.onBackpressureBuffer(N, BufferOverflowStrategy.ERROR);
```

### "Producer prebrz, samo poslednje stanje bitno"

```java
flux.onBackpressureLatest();
```

### "Hoću default vrednost ako padne"

```java
mono.onErrorReturn(default);
```

### "Hoću fallback Publisher ako padne"

```java
mono.onErrorResume(ex -> fallbackMono);
```

### "Hoću da retry-jem timeout-e, sa backoff-om"

```java
mono.retryWhen(Retry.backoff(3, Duration.ofMillis(200))
        .filter(ex -> ex instanceof TimeoutException));
```

### "Hoću da preskočim loš element u batch-u"

```java
flux.flatMap(x -> riskyOp(x).onErrorResume(ex -> Mono.empty()));
```

### Backpressure strategije - cheat sheet

| Strategija | Gubi podatke | Memorija | Tipičan slučaj |
|------------|--------------|----------|----------------|
| BUFFER (unbounded) | ne | raste | mali bursts, znamo da je consumer skoro brz |
| BUFFER (bounded) | po overflow strategiji | bound | produkcija - znamo limit |
| DROP | da, najnovije ne stignu | konstantna | metrike, telemetrija |
| LATEST | da, srednja stanja | konstantna | UI stanje |
| ERROR | tok pada | konstantna | striktni ugovori |

### Error handling - cheat sheet

| Operator | Šta vraća | Tok preživi |
|----------|-----------|-------------|
| `onErrorReturn` | konkretna vrednost | da, kompletira |
| `onErrorResume` | drugi Publisher | da, prebacuje se |
| `onErrorContinue` | preskače element | da, ide dalje |
| `onErrorMap` | drugi exception | ne, samo menja tip |
| `doOnError` | ništa (side-effect) | ne, samo log |

### Retry - cheat sheet

| Pattern | Kad |
|---------|-----|
| `retry(n)` | jednostavan slučaj, prolazna greška, fixed broj pokušaja |
| `Retry.fixedDelay(n, d)` | poznat recovery time |
| `Retry.backoff(n, d).jitter()` | distribuirani sistem, izbegavamo herd |
| `Retry.indefinitely().filter(...)` | reconnect dok ima mreže |

---

## 9. Šta dolazi sledeće nedelje

Nedelja 6 pokriva **praktične obrasce u realnim aplikacijama**: HTTP
klijent (`WebClient`), event bus, deljeno stanje, ETL pipelines. Tu se
sve od nedelje 1-5 sklapa zajedno - operatori, schedulers, backpressure,
retry - u radnu reaktivnu aplikaciju.

| Tema | Šta gradimo |
|------|-------------|
| `WebClient` | reaktivni HTTP klijent, JSON, error handling |
| Event bus | `Sinks.many()` kao publish-subscribe |
| State | `Flux.scan` kao reduktor stanja |
| ETL | čitanje, transformacija, batch upis |

Sledeća nedelja je **najpragmatičnija** - manje teorije, više slaganja
postojećih komponenti.

---

## 10. Primeri koda i vežbe

| Fajl | Tema |
|------|------|
| [`BackpressureStrategiesDemo.java`](BackpressureStrategiesDemo.java) | BUFFER, DROP, LATEST, ERROR, `limitRate` - vidi se razlika u ponašanju |
| [`ErrorHandlingDemo.java`](ErrorHandlingDemo.java) | `onErrorReturn`, `onErrorResume`, `onErrorContinue`, `onErrorMap`, `doOnError` |
| [`RetryDemo.java`](RetryDemo.java) | `retry(n)`, `Retry.fixedDelay`, `Retry.backoff` sa jitter-om, filter po tipu greške |
| [`PracticeTasksForStudents.java`](PracticeTasksForStudents.java) | Zadaci za samostalnu vežbu |
| [`PracticeTasksSolutions.java`](PracticeTasksSolutions.java) | Rešenja zadataka |

---
