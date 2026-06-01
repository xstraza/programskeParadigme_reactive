# Paradigme Programiranja - Nedelja 6
## Praktični obrasci - HTTP klijent, event bus, state, ETL

> Prvih pet nedelja smo gradili **alat**: `Mono`/`Flux`, operatore,
> schedulers, backpressure, retry.
>
> Ova nedelja je **manje teorije, više inženjeringa**. Svaki primer je
> mini-arhitektura: kako se reaktivni kod ponaša kad pređe granicu
> procesa (HTTP), kad treba da podeli događaj između više slušalaca
> (event bus), kad mora da pamti stanje (state holder), ili kad povezuje
> tri servisa u jedan odgovor (combining).

---

## Sadržaj

1. [Reaktivni HTTP klijent](#1-reaktivni-http-klijent)
2. [Event bus - `Sinks.many()`](#2-event-bus--sinksmany)
3. [Reaktivno stanje - `Flux.scan`](#3-reaktivno-stanje--fluxscan)
4. [ETL pipeline - extract / transform / load](#4-etl-pipeline--extract--transform--load)
5. [Kombinovanje izvora - `zip` + `timeout` + `retry`](#5-kombinovanje-izvora--zip--timeout--retry)
6. [Praktični saveti](#6-praktični-saveti)
7. [Brza referenca](#7-brza-referenca)
8. [Šta dolazi sledeće nedelje](#8-šta-dolazi-sledeće-nedelje)
9. [Primeri koda i vežbe](#9-primeri-koda-i-vežbe)

---

## 1. Reaktivni HTTP klijent

Najčešći **side-effect** u realnoj aplikaciji je poziv ka drugom servisu
preko mreže. Klasičan `HttpURLConnection` ili `okhttp` blokira thread
dok čeka odgovor - reaktivni HTTP klijent **ne blokira**: vraća `Mono`
koji se kompletira kada Netty primi odgovor.

U ovom projektu koristimo `reactor.netty.http.client.HttpClient`
(iz `reactor-netty-http`). Spring-ov `WebClient` je *wrapper* iznad
istog Netty-jevog klijenta.

```java
import reactor.netty.http.client.HttpClient;

HttpClient client = HttpClient.create();

Mono<String> odgovor = client.get()
        .uri("https://jsonplaceholder.typicode.com/posts/1")
        .responseContent()
        .aggregate()
        .asString();
```

### Šta `responseContent().aggregate().asString()` znači

- `responseContent()` daje `ByteBufFlux` - tok byte-buffer-a kako stižu
  paketi sa mreže.
- `aggregate()` ih sklapa u jedan buffer (`Mono<ByteBuf>`).
- `asString()` dekodira u UTF-8 string.

Za **streaming** (npr. SSE ili veliki download) ne radimo `aggregate()`,
već radimo na `Flux<String>` ili `Flux<ByteBuf>` redom kako stižu.

### Kombinovanje sa onim što već znamo

HTTP poziv je **prolazan** (može da padne na timeout-u, mreža puca,
server vrati 5xx). Tu se direktno aktivira sve iz nedelje 5:

```java
Mono<String> robustan = client.get()
        .uri(url)
        .responseContent().aggregate().asString()
        .timeout(Duration.ofSeconds(2))
        .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
                .filter(ex -> ex instanceof TimeoutException
                           || ex instanceof IOException))
        .onErrorReturn("FALLBACK");
```

### Paralelni pozivi

Kad treba **istovremeno** pogoditi N endpoint-a, `flatMap` (nedelja 3)
radi posao - svaki poziv je `Mono`, `flatMap` ih pokreće paralelno
preko Netty event-loop-a (bez extra thread-ova!).

```java
Flux.range(1, 10)
    .flatMap(id -> client.get()
            .uri("https://jsonplaceholder.typicode.com/posts/" + id)
            .responseContent().aggregate().asString())
    .collectList()
    .block();
```

> **Demo:** [`httpclient/HttpClientDemo.java`](httpclient/HttpClientDemo.java)

> **Napomena za izvođenje:** demo zahteva internet (gađa
> `jsonplaceholder.typicode.com`). Bez mreže videće se TimeoutException
> kao očekivan rezultat - to je takođe edukativno.

---

## 2. Event bus - `Sinks.many()`

`Flux.create` smo videli u nedelji 2 - **most** između push-izvora i
reaktivnog sveta. `Sinks` su moderna, type-safe verzija istog
koncepta, sa eksplicitnom strategijom šta da se radi sa pretplatama
**posle** prvih elemenata.

### Tri osnovne fabrike

| Fabrika | Ko dobija šta |
|---------|---------------|
| `Sinks.many().unicast()` | **jedan** subscriber prima sve; drugi je odbijen |
| `Sinks.many().multicast().onBackpressureBuffer()` | više subscriber-a; svaki dobija **samo nove** elemente posle pretplate |
| `Sinks.many().replay().all()` | više subscriber-a; svaki dobija **sve od početka**, čak i kasnije pretplaćeni |
| `Sinks.many().replay().limit(n)` | replay, ali samo **poslednjih N** elemenata |

### `emitNext` umesto `next` - bezbedna emisija

```java
Sinks.Many<String> bus = Sinks.many().multicast().onBackpressureBuffer();

bus.emitNext("hello", Sinks.EmitFailureHandler.FAIL_FAST);
bus.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
```

Drugi argument je **politika** za slučaj kad emisija ne uspe
(npr. konkurentni `emitNext` iz dva thread-a). `FAIL_FAST` baca odmah;
u produkciji često napišemo `(signal, result) -> result == EmitResult.FAIL_NON_SERIALIZED`
(retry kratko ako je samo race).

### `asFlux()` - publish strana

Sink ima dve strane: `emitXxx` (produkcija) i `asFlux()` (potrošnja).
Subscriberi se pretplaćuju na `bus.asFlux()`, ne na sam sink.

```java
Flux<String> stream = bus.asFlux();
stream.subscribe(v -> log("sub1", v));
stream.subscribe(v -> log("sub2", v));

bus.emitNext("a", FAIL_FAST);     // oba subscriber-a vide "a"
```

### Kada multicast, kada replay

- **multicast** = "ko stigne taj uzme". Novi subscriber **ne vidi**
  istoriju. Tipično: live event stream (klikovi, notifikacije).
- **replay(all)** = "stigni kad stigneš, dobićeš sve". Memorija raste
  sa brojem emitovanih elemenata - oprezno.
- **replay(n)** = kompromis: poslednjih N. Tipično: chat sa "last 50
  messages" pri otvaranju.

### Mentalni model

Sink je **interna kontrolna ploča** producer-a; `Flux` koji vraća
`asFlux()` je **javni interfejs** za consumer-e. Skriva
implementaciju (`unicast`/`multicast`/`replay`) iza istog `Flux<T>`
tipa - subscriberima je svejedno koja je strategija dok god ugovor
važi.

> **Demo:** [`eventbus/EventBusDemo.java`](eventbus/EventBusDemo.java)

---

## 3. Reaktivno stanje - `Flux.scan`

`scan` je **reduce sa međurezultatima**. `reduce` daje samo finalni
rezultat (`Mono`); `scan` emituje **svaki korak** (`Flux`). To ga čini
prirodnim **state holder-om**: tok komandi unutra, tok stanja napolje.

```java
//        komande                          stanja
//   ──── +1 ───── +5 ───── -2 ───>  ───── 1 ───── 6 ───── 4 ───>
//
Flux<Integer> komande = Flux.just(1, 5, -2);

Flux<Integer> stanja = komande.scan(0, (state, delta) -> state + delta);
//                       0 (seed) -> 1 -> 6 -> 4
```

### Šablon "Redux u Reactor-u"

```java
sealed interface Action permits Add, Remove, Clear {}
record Add(String item) implements Action {}
record Remove(String item) implements Action {}
record Clear() implements Action {}

Sinks.Many<Action> actions = Sinks.many().multicast().onBackpressureBuffer();

Flux<List<String>> state = actions.asFlux()
        .scan(List.<String>of(), (acc, act) -> switch (act) {
            case Add a    -> append(acc, a.item());
            case Remove r -> acc.stream().filter(x -> !x.equals(r.item())).toList();
            case Clear c  -> List.of();
        });

state.subscribe(s -> log("ui", s));

actions.emitNext(new Add("mleko"),    FAIL_FAST);
actions.emitNext(new Add("hleb"),     FAIL_FAST);
actions.emitNext(new Remove("mleko"), FAIL_FAST);
```

Tri elementa:
- **akcije** dolaze kroz `Sinks.Many` (event bus iznad);
- **redukcija** je čista funkcija `(state, action) -> state`;
- **stanja** se emituju kao `Flux<State>` - UI sluša i renderuje.

Reaktivnost je **prirodna** - stanje je tok, ne polje. Svaki subscriber
dobija aktuelno stanje + sve buduće promene (sa `replay(1)` opciono).

### `distinctUntilChanged` - manje render-ovanja

```java
state.distinctUntilChanged()
     .subscribe(s -> redraw(s));
```

Ne emituj uzastopne identične vrednosti - efektivno smanjuje
nepotrebne update-e potrošačima.

> **Demo:** [`state/StateHolderDemo.java`](state/StateHolderDemo.java)

---

## 4. ETL pipeline - extract / transform / load

ETL = **Extract, Transform, Load**. Klasičan batch obrazac, ali sa
reaktivnim operatorima dobija besplatne stvari:
- **paralelnu obradu** (`flatMap`, `parallel`);
- **batching** (`buffer`, `window`);
- **error isolation** (`onErrorResume` per-element);
- **backpressure** (čitamo iz fajla onoliko brzo koliko consumer može).

```
   E             T                       L
[ izvor ] --> [ parse ] --> [ enrich ] --> [ batch ] --> [ upis ]
   |             |             |             |             |
 Flux<Raw>   Flux<Parsed>  Flux<Enriched>  Flux<List>     Mono<Void>
```

### Skeleton

```java
Flux.fromIterable(redovi)                            // E: izvor
    .flatMap(this::parse,        /*concurrency=*/ 4) // T: paralelno parse
    .flatMap(this::enrich,        4)                 // T: paralelno enrich
    .onErrorContinue((ex, item) -> log.warn("skip {}", item))
    .buffer(100)                                     // L: u batch-evima
    .flatMap(this::upisniBatch)
    .then()                                          // Mono<Void> - kraj
    .block();
```

### Zašto `flatMap` sa concurrency-jem

`flatMap(fn, N)` dozvoljava **N istovremenih inner-Mono-a**. Ako parse
traje 100ms i čitamo 1000 redova, sa `concurrency=4` to je 1000/4 ×
100ms = 25s umesto 100s. Bez troška extra thread-ova (sve ide preko
schedulera koji već koristimo).

### Batch upis preko `buffer(N)`

`buffer(100)` skuplja po 100 elemenata u `List<T>` i emituje listu.
Dalje `flatMap` na takvoj listi je idealan za **bulk insert**:

```java
.buffer(100)
.flatMap(batch -> repo.saveAll(batch))   // 1 DB poziv po 100 redova
```

Kad source pravi 100k redova, ovo smanji broj DB poziva 100x. Ovo je
ČESTO razlog što reaktivni pipeline pobeđuje imperativni - ne zbog
thread-ova nego zbog **agregacije I/O poziva**.

### Window-uj po vremenu, ne samo po broju

```java
.windowTimeout(100, Duration.ofSeconds(1))
.flatMap(w -> w.collectList())
.flatMap(this::upisniBatch)
```

"Šalji batch od 100 ILI svake sekunde, šta god prvo." Tipično za
real-time pipeline-e gde ne smemo da držimo elemente predugo.

### Error isolation

`onErrorContinue` (nedelja 5) ili `flatMap` + `onErrorResume(empty)`:
loš red **ne sme** da pokvari ostatak fajla.

```java
.flatMap(red -> obradi(red)
        .onErrorResume(ex -> {
            metrics.brojErrora.incrementAndGet();
            return Mono.empty();
        }))
```

> **Demo:** [`pipeline/EtlPipelineDemo.java`](pipeline/EtlPipelineDemo.java)

---

## 5. Kombinovanje izvora - `zip` + `timeout` + `retry`

Realan endpoint često **agregira** više pozadinskih servisa:

```
GET /dashboard/:user
   ├── GET /profile/:user     (servis A, ~50ms)
   ├── GET /orders/:user      (servis B, ~80ms)
   └── GET /recommendations   (servis C, ~120ms)

vraća: { profile, orders, recommendations }
```

### `Mono.zip` - paralelno + kompozicija

```java
Mono<Dashboard> dashboard = Mono.zip(
        profilSvc.get(userId),
        narudžbineSvc.get(userId),
        preporukeSvc.get(userId))
    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()));
```

Sva tri Mono-a se subscribe-uju **istovremeno**. `zip` čeka **sve tri**
i mapira u rezultat. Vreme = `max(t_a, t_b, t_c)`, ne `t_a + t_b + t_c`.

### Šta ako jedan padne

`Mono.zip` po default-u **propagira grešku** odmah - ako profil padne,
ceo zip pada (čak iako su orders i recommendations već stigli).

Ako želimo **delimično** ponašanje ("dashboard sa praznim
recommendations ako preporuke padnu"):

```java
Mono<Recommendations> bezPada = preporukeSvc.get(userId)
        .onErrorReturn(Recommendations.empty());

Mono<Dashboard> dashboard = Mono.zip(
        profilSvc.get(userId),
        narudžbineSvc.get(userId),
        bezPada)                                    // ovaj ne pada
    .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()));
```

**Pravilo:** odluči po servisu - šta je *kritično* (zip pada ako on
padne) vs *opciono* (lokalni fallback pa zip ne primeti).

### `zip` + `timeout` po servisu

```java
Mono.zip(
    profilSvc.get(userId).timeout(Duration.ofMillis(500))
        .onErrorReturn(Profil.UNKNOWN),
    narudžbineSvc.get(userId).timeout(Duration.ofMillis(500))
        .onErrorReturn(List.of()),
    preporukeSvc.get(userId).timeout(Duration.ofMillis(300))
        .onErrorReturn(List.of()))
.map(...)
.timeout(Duration.ofSeconds(1))                      // overall guard
```

Dva sloja timeout-a:
- **per-servis** sa fallback-om (tihi degrade);
- **overall** kao tvrda gornja granica (ceo dashboard nikad ne traje
  duže od 1s, bez obzira na sve).

### `Mono.zip` vs `Flux.zip` vs `combineLatest`

- `Mono.zip(a, b, c)` - svaki Mono daje **jedan** rezultat, zip čeka sve.
- `Flux.zip(a, b)` - parovi **po indeksu**: prvi-prvi, drugi-drugi.
- `Flux.combineLatest(a, b)` - svaki put kad **bilo koji** emituje,
  kombinuj sa poslednjom vrednošću drugog (mention sa nedelje 3).

> **Demo:** [`combining/CombiningDemo.java`](combining/CombiningDemo.java)

---

## 6. Praktični saveti

### Ne blokiraj na event-loop-u

Reactor Netty deli **mali pool** event-loop thread-ova (po jezgru).
Ako u njima blockiraš (`Thread.sleep`, `JDBC`, file I/O), **ceo HTTP
podsistem stoji**. Pravilo: sve što blokira ide na
`Schedulers.boundedElastic()`:

```java
Mono.fromCallable(() -> jdbcQuery())                 // BLOKIRA
    .subscribeOn(Schedulers.boundedElastic())        // ali na safe pool-u
```

### Logging sa `log()` operator-om

Reactor ima ugrađen operator: `flux.log()` (ili `.log("naziv")`) prikazuje
sve signale (`onSubscribe`, `request`, `onNext`, `onComplete`, `onError`).
Neprocenjivo za debugging - vidi se ko-šta-kome i kojim redom.

```java
Flux.range(1, 3)
    .log("izvor")
    .map(n -> n * 10)
    .log("posle map-a")
    .subscribe();
```

### Imenuj pipeline-e u logovima

```java
Mono<...> task = ...;
task.checkpoint("isplata-poziv")      // markira stack-trace pri grešci
    .subscribe();
```

`checkpoint` ubacuje "lokacija" u stack trace - kad nešto padne 4
operatora kasnije, vidiš odakle je krenulo.

### `Hooks.onOperatorDebug()` u dev-u

```java
public static void main(String[] args) {
    Hooks.onOperatorDebug();         // SAMO u dev/test - mnogo overhead-a
    ...
}
```

Pri svakom operatoru hvata stack-trace. Pri grešci dobijamo "assembly
stack trace" - kod gde je pipeline sastavljen, ne kod gde se signal
širi.

### Resource cleanup - `using`

Za resource koji **mora** da se zatvori (file handle, DB konekcija):

```java
Flux.using(
    () -> openFile(path),                            // resource factory
    file -> Flux.fromStream(file.lines()),           // tok
    file -> closeFile(file)                          // cleanup, uvek pozvan
);
```

Kao `try-with-resources` ali reaktivno. Cleanup teče i na success,
error, i cancel.

---

## 7. Brza referenca

### HTTP klijent

```java
HttpClient.create()
    .get().uri(url)
    .responseContent().aggregate().asString()
    .timeout(Duration.ofSeconds(2))
    .retryWhen(Retry.backoff(3, Duration.ofMillis(200)));
```

### Event bus (multicast)

```java
Sinks.Many<Event> bus = Sinks.many().multicast().onBackpressureBuffer();
bus.asFlux().subscribe(e -> ...);
bus.emitNext(event, Sinks.EmitFailureHandler.FAIL_FAST);
```

### State holder

```java
actions.asFlux()
    .scan(initialState, (state, action) -> reduce(state, action))
    .distinctUntilChanged()
    .subscribe(ui::render);
```

### ETL skeleton

```java
Flux.fromIterable(source)
    .flatMap(this::parse, 4)
    .flatMap(this::enrich, 4)
    .buffer(100)
    .flatMap(this::saveBatch)
    .then().block();
```

### Dashboard / paralelna agregacija

```java
Mono.zip(
    svcA.get().timeout(D).onErrorReturn(defA),
    svcB.get().timeout(D).onErrorReturn(defB),
    svcC.get().timeout(D).onErrorReturn(defC))
  .map(t -> new Dashboard(t.getT1(), t.getT2(), t.getT3()));
```

### Kada koji `Sinks`

| Hoću | Sinks |
|------|-------|
| 1:1 producer i consumer | `unicast().onBackpressureBuffer()` |
| 1:N live (samo novi) | `multicast().onBackpressureBuffer()` |
| 1:N sa istorijom | `replay().all()` ili `replay().limit(N)` |
| 1:N sa "samo zadnja" | `replay().latest()` |

### Kada koji combinator

| Hoću | Operator |
|------|----------|
| čekaj sve Mono-e, mapiraj sve | `Mono.zip(a, b, c)` |
| čekaj **bilo koji** Mono | `Mono.firstWithValue(a, b)` |
| spoji dva `Flux`-a po indeksu | `Flux.zip(a, b)` |
| spoji "kad god ko emituje" | `Flux.combineLatest(a, b, fn)` |
| nadovezi dva `Flux`-a redno | `Flux.concat(a, b)` |
| pomešaj `Flux`-eve po dolasku | `Flux.merge(a, b)` |

---

## 8. Šta dolazi sledeće nedelje

Nedelja 7 zatvara semestar:

- **Testiranje** sa `StepVerifier` - kako se piše assert nad tokom?
- **`VirtualTimeScheduler`** - testiranje koda sa `delay`/`interval` bez
  realnog čekanja.
- **Hot vs cold streams**, `share`/`cache`/`replay` na operator-skom
  nivou.
- Sveobuhvatne vežbe pred kolokvijum.

---

## 9. Primeri koda i vežbe

| Fajl | Tema |
|------|------|
| [`httpclient/HttpClientDemo.java`](httpclient/HttpClientDemo.java) | `HttpClient` GET, JSON, paralelni pozivi, timeout+retry |
| [`eventbus/EventBusDemo.java`](eventbus/EventBusDemo.java) | `Sinks.many` - unicast/multicast/replay, više subscriber-a |
| [`state/StateHolderDemo.java`](state/StateHolderDemo.java) | `Flux.scan` kao reduktor, Redux-stil obrazac |
| [`pipeline/EtlPipelineDemo.java`](pipeline/EtlPipelineDemo.java) | ETL: parse → enrich → batch → load, sa error isolation-om |
| [`combining/CombiningDemo.java`](combining/CombiningDemo.java) | `Mono.zip` + per-service timeout/fallback + overall timeout |
| [`practice/PracticeTasksForStudents.java`](practice/PracticeTasksForStudents.java) | Zadaci za samostalnu vežbu |
| [`practice/PracticeTasksSolutions.java`](practice/PracticeTasksSolutions.java) | Rešenja zadataka |

---
