# Paradigme Programiranja - Nedelja 1
## Uvod u reaktivno programiranje, Reactive Streams i Java Flow API

---

## Sadržaj

1. [Zašto reaktivno programiranje?](#1-zašto-reaktivno-programiranje)
2. [Push vs. Pull model](#2-push-vs-pull-model)
3. [Reactive Streams specifikacija](#3-reactive-streams-specifikacija)
4. [Java Flow API - most ka Reactive Streams](#4-java-flow-api--most-ka-reactive-streams)
5. [Stream API vs. Reaktivni stream](#5-stream-api-vs-reaktivni-stream)
6. [Prednosti i zamke reaktivnog pristupa](#6-prednosti-i-zamke-reaktivnog-pristupa)
7. [Šta dolazi sledeće - Project Reactor](#7-šta-dolazi-sledeće--project-reactor)
8. [Primeri koda](#8-primeri-koda)

---

## 1. Zašto reaktivno programiranje?

U prvoj polovini semestra naučili smo **funkcionalno programiranje** u Javi:
lambda izraze, Stream API, `Optional`, kompoziciju funkcija. Stream API je
moćan alat - ali ima jedno ograničenje:

> **Stream radi nad podacima koji već postoje u memoriji.** Operacije
> su sinhrone, blokirajuće i pull-bazirane (terminalna operacija
> "vuče" elemente jedan po jedan).

A šta sa podacima koji **dolaze tokom vremena**? Šta sa pozivima ka
mreži, bazi, fajlu, ili event stream-om sa korisničkog interfejsa? Tu
klasični Stream postaje nezgodan - ili ćemo blokirati nit dok čekamo,
ili ćemo žonglirati sa `CompletableFuture`, `Future`, callback-ovima,
i veoma brzo upadati u **callback hell**.

### Problem 1: blokirajući I/O

```java
// Klasičan blokirajući stil - nit "spava" dok čeka odgovor
String odgovor = httpKlijent.GET("https://api.example.com/users");
List<User> users = parsiraj(odgovor);
```

Dok čekamo odgovor, čitava nit je **zauzeta** i ne radi ništa korisno.
Web server koji obrađuje 10.000 konkurentnih veza klasičnim
blokirajućim modelom mora da otvori 10.000 niti - što troši ogromno
memorije (~1 MB stack po niti) i CPU vremena na context switching.

### Problem 2: callback hell

Kad pređemo na asinhroni stil, dobijamo:

```java
// Pseudo-kod - asinhroni, ali nečitljiv
httpKlijent.GETasync("/users", users -> {
    for (User u : users) {
        bazaKlijent.findOrdersAsync(u.id, orders -> {
            for (Order o : orders) {
                emailServis.sendAsync(u.email, o, ok -> {
                    if (!ok) loger.errorAsync(...);
                });
            }
        });
    }
});
```

Logika je razbacana po nivoima ugnežđavanja. Greška u jednom callback-u
ne propagira automatski naviše. Testiranje je teško.

### Rešenje: reaktivno programiranje

**Reaktivno programiranje** pruža apstrakciju nad asinhronim tokom
podataka kroz vreme - istu kompozicionu eleganciju koju ima Stream
API, ali nad **asinhronim, push-baziranim** izvorima:

```java
// Kompozicija ostaje deklarativna, ali sve je asinhrono
httpKlijent.get("/users")               // Mono<List<User>> ili Flux<User>
    .flatMap(user -> bazaKlijent.findOrders(user.id))
    .flatMap(order -> emailServis.send(order))
    .doOnError(loger::error)
    .subscribe();
```

Ovo nije magija - ovo je `Stream` koji ume da se nosi sa **vremenom**.

---

## 2. Push vs. Pull model

Razlika između klasičnih kolekcija/stream-ova i reaktivnih tokova
najčistije se vidi kroz pitanje "ko kontroliše tempo".

### Pull model (klasičan)

Klijent (potrošač) **vuče** podatke iz izvora kada je njemu zgodno:

```
[Potrošač]  ──poziv next()──▶  [Izvor]
[Potrošač]  ◀─vraća element──  [Izvor]
[Potrošač]  ──poziv next()──▶  [Izvor]
[Potrošač]  ◀─vraća element──  [Izvor]
   ...
```

Karakteristike:
- Sinhrono, blokirajuće.
- Potrošač kontroliše tempo - ako je spor, izvor "čeka".
- Primer: `Iterator.next()`, `BufferedReader.readLine()`, `Stream.forEach()`.

### Push model (reaktivni)

Izvor **gura** podatke ka potrošaču čim su dostupni:

```
[Izvor]  ──onNext(elem1)──▶  [Potrošač]
[Izvor]  ──onNext(elem2)──▶  [Potrošač]
[Izvor]  ──onNext(elem3)──▶  [Potrošač]
[Izvor]  ──onComplete()──▶   [Potrošač]
```

Karakteristike:
- Asinhrono.
- Izvor kontroliše tempo - može biti "brži" od potrošača.
- Mora imati mehanizam **backpressure**-a (kontrola da brz izvor ne
  preplavi sporog potrošača).
- Primer: GUI događaji, mrežni socket, sensor stream, event bus.

### Kombinacija - pull-push

Reactive Streams specifikacija zapravo kombinuje oba: **potrošač
zahteva** koliko elemenata je spreman da primi (`request(n)`), pa
**izvor gura** najviše toliko (`onNext`). Ovo je **dynamic
backpressure** - push semantika sa pull kontrolom tempa.

> **Demo:** [`PushVsPullModel.java`](PushVsPullModel.java) ilustruje oba
> modela paralelno, sa istim podacima.

---

## 3. Reactive Streams specifikacija

**Reactive Streams** je standard razvijen 2013-2015. od strane Netflix-a,
Pivotal-a, Lightbend-a i drugih (postao je deo JDK od Jave 9 kao
`java.util.concurrent.Flow`). Definiše **četiri interfejsa** i set
**pravila** koje implementacija mora da poštuje.

### Četiri interfejsa

| Interfejs | Uloga |
|-----------|-------|
| `Publisher<T>` | Izvor podataka - emituje 0..N elemenata, pa onComplete ili onError |
| `Subscriber<T>` | Potrošač - prima signale: `onSubscribe`, `onNext`, `onError`, `onComplete` |
| `Subscription` | Veza između Publisher-a i Subscriber-a - preko nje Subscriber traži (`request`) ili otkazuje (`cancel`) |
| `Processor<T,R>` | Ujedno Publisher i Subscriber - operator koji transformiše tok (interno se ovo retko piše ručno) |

### Lifecycle signala

Svaki Subscriber tačno jednom prolazi kroz sledeći redosled signala:

```
onSubscribe(Subscription)    ← uvek prvi
   ↓
onNext(elem)*                ← 0 ili više puta
   ↓
onComplete()  XOR  onError(Throwable)    ← tačno jedan terminalni signal
```

Pravila ukratko (selekcija, ima ih ~30):

> 1. Publisher mora signalizirati `onNext` najviše onoliko koliko je
>    Subscriber zatražio sa `request(n)` (backpressure).
> 2. `onComplete` i `onError` su **terminalni** - nema više signala posle.
> 3. `onSubscribe` se mora pozvati pre bilo kog drugog signala.
> 4. Sve metode su sekvencijalne za jednog Subscriber-a (happens-before).
> 5. Subscription je **thread-safe** - `request` i `cancel` mogu doći iz
>    bilo koje niti.

### Veza sa Java Flow API

Java 9+ ima `java.util.concurrent.Flow` - sadrži tačno te iste četiri
interfejsa, identične potpise, da bi standardna biblioteka mogla biti
"jezik" za sve reaktivne implementacije (Reactor, RxJava, Akka Streams).

> **Demo:** [`ReactiveStreamsSpecification.java`](ReactiveStreamsSpecification.java)
> implementira mali `Publisher` i `Subscriber` ručno da se vidi kako
> protokol funkcioniše bez biblioteke.

---

## 4. Java Flow API - vesa sa Reactive Streams

`java.util.concurrent.Flow` je deo standardne biblioteke. Sadrži:

- `Flow.Publisher<T>`
- `Flow.Subscriber<T>`
- `Flow.Subscription`
- `Flow.Processor<T, R>`
- `SubmissionPublisher<T>` - gotova implementacija Publisher-a koju
  možemo koristiti za "guranje" podataka.

```java
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;

try (var publisher = new SubmissionPublisher<String>()) {
    publisher.subscribe(novSubscriber);
    publisher.submit("Zdravo");
    publisher.submit("Reaktivni svet");
}   // close() šalje onComplete
```

Flow API je **dovoljan za razumevanje protokola**, ali u praksi se ne
piše direktno - nedostaju mu operatori (`map`, `filter`, `flatMap`,
itd). Od sledeće nedelje prelazimo na **Project Reactor** koji nadograđuje
Flow API bogatim setom operatora i schedulera.

> **Demo:** [`JavaFlowApiDemo.java`](JavaFlowApiDemo.java) - koristi
> `SubmissionPublisher` i `Flow.Subscriber` za realan, runnable primer.

---

## 5. Stream API vs. Reaktivni stream

Iako liče po sintaksi, **Stream** i **Flux/Mono** rešavaju različite
probleme. Tabela rezimira ključne razlike:

| Aspekt | `Stream<T>` (FP deo) | `Flux<T>` / `Mono<T>` (reaktivno) |
|--------|----------------------|-----------------------------------|
| Model | pull (terminalna operacija vuče) | push sa backpressure-om |
| Sinhrono / asinhrono | sinhrono | asinhrono podrazumevano |
| Vreme između elemenata | nema (sve odmah) | prvoklasni concept (delay, interval) |
| Ponovno korišćenje | jednokratno (terminalna troši) | može se subscribe-ovati više puta (cold) |
| Greška | iznosi se kao izuzetak | signal `onError` u toku |
| Kontekst niti | nit poziva | scheduler bira nit (`subscribeOn`, `publishOn`) |
| Backpressure | N/A | ugrađeno (`request(n)`) |
| Tipičan ulaz | `Collection`, niz, fajl već u memoriji | mreža, događaji, baza, vreme |
| Operatori | `map`, `filter`, `flatMap`, `reduce`, ... | **iste**, plus `merge`, `zip`, `retry`, `timeout`, ... |

Ključna ideja: **operator imena imaju isto značenje**, što je
pedagoški most. `map` u Stream-u je `T → R` po elementu; `map` u
Flux-u je takođe `T → R` po elementu - samo što elementi mogu doći
asinhrono u različitim trenucima.

```java
// Stream - sve odmah, sinhrono
List.of(1, 2, 3, 4)
    .stream()
    .filter(n -> n % 2 == 0)
    .map(n -> n * 10)
    .forEach(System.out::println);   // 20, 40

// Flux - vremenski razvučeno, asinhrono
Flux.just(1, 2, 3, 4)
    .filter(n -> n % 2 == 0)
    .map(n -> n * 10)
    .delayElements(Duration.ofMillis(200))
    .subscribe(System.out::println); // 20 (200ms), 40 (400ms)
```

> **Demo:** [`StreamVsReactive.java`](StreamVsReactive.java) - paralelno
> isti pipeline u Stream-u i u Flux-u, da se vidi razlika u ponašanju.

---

## 6. Prednosti i zamke reaktivnog pristupa

### Kada reaktivno **pomaže**

- Aplikacija troši mnogo na **I/O čekanje** (mreža, baza, fajl).
- Podaci dolaze **postepeno** kroz vreme (event bus, sensor, GUI).
- Treba kombinovati **više asinhronih izvora** (npr. tri API poziva
  paralelno, čekanje na sve, zip).
- Potrebna nam je **otpornost** - retry sa exponential backoff, timeout,
  fallback, circuit breaker.
- Microservice arhitektura sa mnogo paralelnih klijenata po jednom
  serveru (Spring WebFlux).

### Kada reaktivno **ne pomaže** (ili odmaže)

- **CPU-bound** posao - reaktivni model ne ubrzava CPU; čak unosi
  overhead. Za parsiranje, kompresiju, kriptografiju - koristi
  klasičan stream ili `parallel()`.
- **Mali, sinhroni skripting** - overhead je veći od koristi.
- **Tim koji ne razume model** - reaktivni stack je teže debug-ovati;
  stack trace ne prati klasične niti.

### Zamke koje treba pomenuti odmah

> **Zamka 1:** `subscribe()` se *mora pozvati* da bi se išta desilo.
> Mono/Flux su **lenji** - sastavljanje pipeline-a je samo nacrt.

> **Zamka 2:** Blokirati u operatoru (`Thread.sleep`, sinhroni I/O,
> `block()` unutar `map`) je gotovo uvek bug - blokirate nit
> schedulera koji nije za to namenjen.

> **Zamka 3:** Side-effects pripadaju u `doOnNext` / `doOnError` /
> `doOnSubscribe`, **ne** u `map`. `map` mora biti **čista funkcija**.

> **Zamka 4:** Test sa `Thread.sleep` je flaky. Koristi `StepVerifier`
> i `VirtualTimeScheduler` (videćemo u nedeljama koje dolaze).

---

## 7. Šta dolazi sledeće - Project Reactor

**Project Reactor** je biblioteka koja implementira Reactive Streams
specifikaciju i dodaje:

- **`Mono<T>`** - tok od 0 ili 1 elementa (~ `Optional<CompletableFuture<T>>`).
- **`Flux<T>`** - tok od 0..N elemenata.
- Bogat set operatora: `map`, `filter`, `flatMap`, `merge`, `zip`,
  `groupBy`, `window`, `retry`, `timeout`, ...
- `Schedulers` - eksplicitna kontrola niti (`parallel`, `boundedElastic`,
  `single`).
- Integracija sa Spring WebFlux, R2DBC, reactor-netty, ...

Project Reactor ćemo koristiti od sledeć nedelj. Ova
nedelja je teorijski temelj - najbitnije je razumeti Publisher/Subscriber i
zašto je tok 'push', sve ostalo je sintaksa.

```java
// Sneak peek - nedelja 2:
import reactor.core.publisher.Flux;

Flux.range(1, 10)
    .filter(n -> n % 2 == 0)
    .map(n -> n * n)
    .subscribe(System.out::println);   // 4, 16, 36, 64, 100
```

> **Demo:** [`Introduction.java`](Introduction.java) - minimalan
> "hello reactive world" sa Flux-om

---

## 8. Primeri koda

| Fajl | Tema |
|------|------|
| [`PushVsPullModel.java`](PushVsPullModel.java) | Pull (Iterator) vs. push (callback / Observer) - konceptualna ilustracija |
| [`ReactiveStreamsSpecification.java`](ReactiveStreamsSpecification.java) | Ručna implementacija `Publisher` / `Subscriber` / `Subscription` (Flow API) |
| [`JavaFlowApiDemo.java`](JavaFlowApiDemo.java) | `SubmissionPublisher` i Flow.Subscriber u akciji - runnable primer |
| [`StreamVsReactive.java`](StreamVsReactive.java) | Isti pipeline kao `Stream<T>` i kao `Flux<T>` - razlika u ponašanju |
| [`Introduction.java`](Introduction.java) | "Hello reactive world" sa Project Reactor-om - sneak peek za nedelju 2 |

---
