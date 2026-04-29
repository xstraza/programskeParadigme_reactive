# Paradigme Programiranja - Nedelja 1
## Uvod u reaktivno programiranje, Reactive Streams i Java Flow API

---

## Sadržaj

1. [Priča za početak: ekran sa tri izvora](#1-priča-za-početak-ekran-sa-tri-izvora)
2. [Evolucija: kako smo došli dovde](#2-evolucija-kako-smo-došli-dovde)
3. [Šta je reaktivno programiranje](#3-šta-je-reaktivno-programiranje)
4. [Push vs. Pull model](#4-push-vs-pull-model)
5. [Reactive Streams specifikacija](#5-reactive-streams-specifikacija)
6. [Java Flow API - vesa sa Reactive Streams](#6-java-flow-api---vesa-sa-reactive-streams)
7. [Stream API vs. Reaktivni stream](#7-stream-api-vs-reaktivni-stream)
8. [Prednosti i zamke reaktivnog pristupa](#8-prednosti-i-zamke-reaktivnog-pristupa)
9. [Šta dolazi sledeće - Project Reactor](#9-šta-dolazi-sledeće---project-reactor)
10. [Primeri koda](#10-primeri-koda)

---

## 1. Priča za početak: ekran sa tri izvora

Pre nego što išta definišemo, hajde da uđemo u jednu konkretnu situaciju.
Pravimo mobilnu aplikaciju, sličnu Twitteru ili Instagramu. Korisnik
otvori ekran sa svojim feed-om, i tu treba da se prikaže:

- **profil korisnika** (ime, slika, broj pratilaca) - jedan REST poziv
- **lista poslednjih 10 postova** - drugi REST poziv ka backend-u
- **broj like-ova i komentara za svaki post** - po dva dodatna poziva po
  postu, dakle 20 poziva ukupno

To je 22 nezavisna poziva ka serveru. Svaki traje neko realno vreme:

| Poziv | Tipično vreme |
|-------|---------------|
| `/me` | 200 ms |
| `/me/posts` | 300 ms |
| `/likes/{id}` (×10) | 50 ms svaki |
| `/comments/{id}` (×10) | 50 ms svaki |

### Naivno (sinhrono) rešenje

Ako bismo to napisali "kao što smo navikli" - jedno za drugim:

```java
String profil = http.get("/me");                     // 200 ms
List<Post> postovi = http.get("/me/posts");          // 300 ms
for (Post p : postovi) {
    p.likes = http.get("/likes/" + p.id);            // 10 × 50 = 500 ms
    p.comments = http.get("/comments/" + p.id);     // 10 × 50 = 500 ms
}
prikaziUI(profil, postovi);
```

Ukupno vreme: **200 + 300 + 500 + 500 = 1500 ms**.

Korisnik gleda u prazan ekran 1.5 sekunde pre nego što išta vidi.
Aplikacija deluje sporo iako svaki *pojedinačni* poziv traje
sasvim normalno.

### Šta bismo *želeli*

Pošto su pozivi međusobno nezavisni - mogli bi da idu **paralelno**:

```
profil i postovi  ────────────────▶  max(200, 300) = 300 ms
       ↓
likes svih 10 i komentari svih 10 paralelno ─▶  ~ 50 ms
       ↓
prikaz UI-ja
```

Sa paralelizacijom: **~ 350 ms**, više nego 4x brže. A to je samo *minimum*
- realan UX traži još više:

- prikaz "skeleton" loading state-a dok podaci stižu
- automatski **retry** ako neki poziv padne (mreža je nepouzdana)
- **timeout** ako server ne odgovori za 2 sekunde
- **paginacija** dok korisnik skroluje
- **cancel** ako korisnik napusti ekran pre nego što sve stigne

Klasični sinhroni model **ne ume** ništa od ovoga lepo. Svaki poziv
zauzima čitavu nit dok čeka odgovor; logika za retry / timeout /
cancel se razbacuje po listenerima i `try/catch` blokovima.

> Reaktivno programiranje je **alat napravljen baš za ovaj scenario**:
> komponovanje više asinhronih izvora podataka kroz vreme, sa otpornošću
> i kontrolom resursa.

Hajde sad da vidimo *kako smo došli* do tog alata - jer on nije pao s neba.

---

## 2. Evolucija: kako smo došli dovde

Reaktivno programiranje nije revolucija već *evolutivni odgovor* na
probleme koje smo sretali u svakom prethodnom modelu konkurencije.
Sledeća četiri "doba" pokazuju put.

### Era 1: jedna nit, sinhrono blokiranje

Najstariji model - sve radi jedna nit, blokirajući I/O.

```java
String odgovor = http.get("https://api.example.com/users");  // <- nit "spava" 200ms
List<User> users = parsiraj(odgovor);
```

Dok čekamo odgovor, **čitava nit je zauzeta i ne radi ništa korisno**.
Ako server treba da opsluži još jednog klijenta paralelno - ne može,
nit je blokirana.

**Problem:** ne skalira. Više klijenata = sporija aplikacija.

### Era 2: nit po zahtevu

Rešenje: za svaki dolazni zahtev, otvori novu nit. Tako klasičan
Tomcat / servlet container radi.

```java
new Thread(() -> {
    String odgovor = http.get("/me");
    prikaziProfil(odgovor);
}).start();
```

Bolje! Sad više klijenata radi paralelno.

**Problem:** svaka JVM nit troši ~1 MB stack memorije i mrvi CPU
kroz context switching. Web server koji obrađuje 10.000 konkurentnih
veza klasičnim "thread-per-request" modelom mora da otvori 10.000 niti
- to je ~10 GB samo za stack-ove. Plus sinhronizacija deljenog stanja
između niti je teška i greškama sklona.

> **Pravilo iz prakse:** niti su skupa apstrakcija. Treba da ih ima malo, i
> treba da rade *stalno*, ne da spavaju na I/O.

### Era 3: Future i CompletableFuture

Java 5 dodaje `Future<T>`, Java 8 `CompletableFuture<T>` - asinhroni
poziv koji *ne blokira* nit dok čeka rezultat.

```java
CompletableFuture<String> profil = http.getAsync("/me");
profil.thenAccept(p -> prikaziProfil(p));
```

Mnogo bolje - ista nit može da radi nešto drugo dok server odgovara.

Ali kad treba da *lančamo* više asinhronih operacija, dobijamo:

```java
http.getAsync("/me")
    .thenCompose(profil -> http.getAsync("/posts/" + profil.id))
    .thenCompose(postovi -> dohvatiLikesIComentare(postovi))
    .thenApply(this::pripremiZaUI)
    .exceptionally(this::prikaziGresku)
    .thenAccept(this::prikaziUI);
```

Ili još gore, klasičan **callback hell**:

```java
http.getAsync("/me", users -> {
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

**Šta još fali:**

- `CompletableFuture<T>` predstavlja **tačno jednu** vrednost. A šta sa
  *tokovima* - npr. lista postova koja stiže paginirana?
- Nema **backpressure**-a - ako proizvođač gura brže nego što potrošač
  može, podaci se gomilaju u memoriji.
- Nema standardnih **operatora** za rad sa tokovima (`map`, `filter`,
  `merge`, `zip`, `retry`, `timeout` - sve to bi morali ručno).

### Era 4: Reactive Streams

Generalizacija `CompletableFuture` na **tok od 0..N vrednosti** kroz
vreme, sa backpressure-om i bogatim setom standardnih operatora -
istih onih koje smo videli na `Stream`-u u prvoj polovini semestra.

```java
http.get("/me")                              // Mono<Profil>
    .flatMap(profil ->                       // za svaki profil:
        http.get("/posts/" + profil.id))     //   dovedi listu postova
    .flatMap(post -> Mono.zip(               // za svaki post paralelno:
        http.get("/likes/" + post.id),       //   dovedi like-ove
        http.get("/comments/" + post.id)))   //   i komentare
    .timeout(Duration.ofSeconds(2))          // ako traje > 2s, error
    .retry(3)                                // pokušaj 3 puta na grešku
    .subscribe(this::prikaziPost);           // tek sad sve "kreće"
```

Pet-šest linija, sa retry-em, timeout-om i paralelizacijom uračunatim
"besplatno". Sintaksa = `Stream` koji znamo iz prvog dela, **ali
prošireni vremenom i asinhronošću**.

Tabela rezimira put:

| Era | Model | Problem koji ostaje |
|-----|-------|---------------------|
| 1 - sinhrono | jedna nit blokira na I/O | ne skalira |
| 2 - thread per request | po nit za svaki zahtev | niti su skupe; sinhronizacija teška |
| 3 - Future / Callback | asinhroni *jedan* rezultat | callback hell, nema tokova, nema backpressure-a |
| 4 - Reactive Streams | asinhroni **tok 0..N**, sa kontrolom tempa | (ovo je gde smo sad) |

Svaka era je rešavala problem prethodne. Reaktivno programiranje nije
"alternativa OOP-u" - to je **konkurentnostni model**, sledeći korak u
istoj liniji.

---

## 3. Šta je reaktivno programiranje

Sada možemo dati definiciju koja je dovoljno precizna za ovaj kurs:

> **Reaktivno programiranje** je deklarativni stil rada sa
> **asinhronim tokovima podataka kroz vreme**, sa backpressure-om
> i kompozicijom operatora.

Razložimo svaku reč:

| Termin | Šta znači |
|--------|-----------|
| **deklarativni** | opisujemo *šta* želimo, ne *kako* (kao Stream API) |
| **asinhroni** | ne blokiramo nit dok čekamo rezultat |
| **tokovi (streams)** | sekvenca od 0..N vrednosti koja stiže kroz vreme |
| **kroz vreme** | elementi NE moraju biti svi spremni unapred |
| **backpressure** | potrošač kontroliše tempo da ga izvor ne preplavi |
| **kompozicija operatora** | lančamo `map`, `filter`, `flatMap`, `merge`, `zip`... |

### Šta reaktivno programiranje **nije**

- Nije magija koja ubrzava CPU-bound poslove. Reactive ne ubrzava
  računanje - ubrzava *čekanje*.
- Nije zamena za FP iz prvog dela. **Gradi se nad FP-om** - operatori su
  čiste funkcije, vrednosti su imutabilne.
- Nije rešenje za sve. Mali sinhroni skript ili CPU intenzivan posao
  *ne* treba da budu reaktivni - overhead je veći od koristi.
- Nije nova paradigma za pamćenje od nule. Operatori (`map`, `filter`,
  `flatMap`, `reduce`) imaju **isto značenje** kao na Stream-u.

### Šta jeste

Setimo se ekrana sa tri izvora iz sekcije 1. Reaktivno programiranje
je **alat za baš taj problem**:

- više nezavisnih I/O izvora
- različite brzine (network je nepredvidljiv)
- kombinacije (zip, merge, paralelno)
- otpornost (timeout, retry, fallback)
- kontrola resursa (jedna nit servisira hiljade konekcija)

Ostatak nedelje gradi *minimalni mentalni model* tog alata:
push umesto pull, signali, Subscription, backpressure. Od nedelje 2
prelazimo na Project Reactor i pišemo realan kod.

---

## 4. Push vs. Pull model

Najbrži način da se shvati razlika između klasičnih kolekcija i
reaktivnih tokova je pitanje: **ko kontroliše tempo?**

### Pull model - klijent vuče

Klijent (potrošač) **vuče** podatke iz izvora kada je njemu zgodno:

```
[Potrošač]  ──poziv next()──▶  [Izvor]
[Potrošač]  ◀─vraća element──  [Izvor]
[Potrošač]  ──poziv next()──▶  [Izvor]
[Potrošač]  ◀─vraća element──  [Izvor]
   ...
```

> **Analogija - supermarket:** uđeš u prodavnicu, uzmeš korpu, biraš
> stvari sa polica svojim tempom. Polica te ne juri. *Ti diktiraš tempo.*

Karakteristike:
- sinhrono, blokirajuće
- potrošač kontroliše tempo - ako je spor, izvor "čeka"
- primer: `Iterator.next()`, `BufferedReader.readLine()`, `Stream.forEach()`

### Push model - izvor gura

Izvor **gura** podatke ka potrošaču čim su dostupni:

```
[Izvor]  ──onNext(elem1)──▶  [Potrošač]
[Izvor]  ──onNext(elem2)──▶  [Potrošač]
[Izvor]  ──onNext(elem3)──▶  [Potrošač]
[Izvor]  ──onComplete()──▶   [Potrošač]
```

> **Analogija - restoran:** sediš za stolom, kuhinja šalje jelo kad
> je gotovo. Ti ne kontrolišeš tačno kad stiže šta. Ako šef kuhinje
> pošalje 5 jela odjednom, moraš nekako da se snađeš.

Karakteristike:
- asinhrono
- izvor kontroliše tempo - može biti "brži" od potrošača
- mora postojati neki mehanizam **backpressure**-a (kontrola da brz
  izvor ne preplavi sporog potrošača)
- primer: GUI događaji, mrežni socket, sensor stream, event bus

### Kombinacija - pull-push (Reactive Streams)

Reactive Streams specifikacija zapravo kombinuje oba:
**potrošač zahteva** koliko elemenata je spreman da primi
(`request(n)`), pa **izvor gura** najviše toliko (`onNext`). Ovo je
**dynamic backpressure** - push semantika sa pull kontrolom tempa.

> **Analogija - novine sa pretplatom:** pretplatiš se na novine.
> Stižu ti dnevno. Ako ideš na odmor, pošalješ poruku da
> *pauziraju* - i pauziraju. Kad se vratiš, kažeš "nastavi". Možeš i
> da otkažeš pretplatu.

Tabela poređenja sva tri:

| Aspekt | Pull (supermarket) | Naivni Push (restoran) | Reactive Streams (novine) |
|--------|---------|---------|---------|
| Ko diktira tempo | potrošač | izvor | potrošač - preko `request(n)` |
| Šta ako je potrošač spor | izvor čeka | gomilanje, OOM | izvor staje dok potrošač ne traži |
| Šta ako je izvor spor | potrošač blokira | potrošač čeka | potrošač čeka, ali ne blokira nit |
| Sinhrono / async | sinhrono | async | async |
| Primeri | `Iterator`, `Stream` | klasičan Observer, GUI | `Mono`, `Flux`, RxJava, Akka |

> **Demo:** [`PushVsPullModel.java`](PushVsPullModel.java) ilustruje
> pull i (naivni) push modela paralelno, sa istim podacima i sa malim
> mini-operatorima (`map`, `filter`).

---

## 5. Reactive Streams specifikacija

### Kratka priča: zašto je standard uopšte nastao

Vratimo se u 2013. Reaktivno programiranje na JVM-u tada već postoji,
ali **svaka biblioteka ima svoj API**:

- **RxJava** (Netflix) emituje preko `Observable<T>` sa `subscribe(Observer)`.
- **Akka Streams** (Lightbend) ima svoj `Source<T, Mat>` sa drugačijim
  lifecycle-om.
- **Reactor** (Pivotal/Spring) je tek u nastanku, sa svojim `Flux` i `Mono`.
- Ulaz/izlaz biblioteke (npr. baza, mreža) - ko zna kako su realizovane.

Problem: ako nam jedna biblioteka daje `Observable`, a druga očekuje
`Source`, **moramo ručno da prevodimo**. Plus svaka ima svoju varijantu
backpressure-a, svoju semantiku za grešku, svoj način da ih pomeriš
između niti. Dva tima koja koriste različite biblioteke ne mogu
direktno da povežu kod.

Inženjeri iz Netflix-a, Lightbend-a, Pivotal-a, Red Hat-a, Twitter-a
i drugih su 2013. seli i napravili **minimum dogovor** - četiri
interfejsa i ~30 pravila. **Final 1.0 specifikacija** je objavljena
2015. Naredne godine Java 9 (JEP 266) je *inkorporirala iste interfejse*
u JDK pod `java.util.concurrent.Flow`.

> **Pouka:** Reactive Streams **nije** biblioteka. To je *kontraktni
> minimum* - dovoljno da različite biblioteke mogu da razgovaraju,
> ne više. Pravu funkcionalnost (operatori, schedulers) dobijaš tek
> uz Reactor / RxJava / Akka.

### Mentalni model: lifecycle kao telefonski poziv

Pre formalnih definicija, jedna analogija. Cela komunikacija između
Publisher-a i Subscriber-a liči na telefonski poziv:

| Korak telefonskog poziva                                 | Reactive Streams signal |
|----------------------------------------------------------|------------------------|
| Biraš broj (`subscribe`)                                 | `publisher.subscribe(subscriber)` |
| Druga strana se javi                                     | `onSubscribe(subscription)` |
| Subscription je "kontrola" - reci "spusti", "zovi nazad" | `subscription.request(n)` / `subscription.cancel()` |
| Razgovor traje                                           | `onNext(elem)` više puta |
| Neko spusti slušalicu lepo                               | `onComplete()` |
| Veza pukne (greška)                                      | `onError(throwable)` |
| Nakon spuštanja, ne možete više da pričate               | nema više signala posle terminalnog |

Kada Subscriber kaže `subscription.request(5)`, to je kao da je rekao
"može da mi pričaš još 5 rečenica, posle toga čekaj". Publisher tačno
zna granicu - ne sme da gurne 6. tu rečenicu pre nego što stigne novi
`request()`.

Ovo je ceo protokol. Sve ostalo (`map`, `filter`, `merge`, `zip`...) je
sintaksa nad ovim - operatori implementiraju i Publisher i Subscriber
istovremeno (zato `Processor`).

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

## 6. Java Flow API - veza sa Reactive Streams

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

## 7. Stream API vs. Reaktivni stream

Iako liče po sintaksi, **Stream** i **Flux/Mono** rešavaju različite
probleme. Najlakše se to vidi kroz dva paralelna scenarija - jedan
prirodno odgovara Stream-u, drugi prirodno odgovara Flux-u.

### Scenario A: analiza log fajla (Stream)

Imamo `app.log` od 200 MB. Cilj: prebrojati koliko je `ERROR` linija
upisano *danas*.

```java
String today = LocalDate.now().toString();

long brojGresaka = Files.lines(Path.of("app.log"))
        .filter(line -> line.contains("ERROR"))
        .filter(line -> line.startsWith(today))
        .count();

System.out.println("Greški danas: " + brojGresaka);
```

Karakteristike ovog scenarija:

- Podaci su **već svi tu** - fajl postoji na disku.
- Operacija ima **kraj** - kad pročitamo poslednji red, gotovo.
- Tempo diktira **terminalna operacija** (`count()` "vuče" red po red).
- Sve se odvija na **jednoj niti**.
- Greška = izuzetak (npr. `IOException`).

To je **pull**, sinhrono, jedna nit. **Stream je tu kralj** -
reaktivno bi bilo overkill.

### Scenario B: live alert na cenu akcija (Flux)

Drugi scenario: brokerska aplikacija. Cilj: kad cena AAPL pređe $200,
pošalji push notifikaciju, ali ne više od 1× u 5 minuta.

```java
brokerage.priceStream("AAPL")                          // Flux<Cena> - emituje cenu na svaki tick
        .filter(cena -> cena.amount() > 200.00)        // samo cene preko praga
        .sample(Duration.ofMinutes(5))                 // ne više od 1 alarma u 5 min
        .map(cena -> "AAPL = $" + cena.amount())
        .doOnError(err -> log.error("Stream pukao", err))
        .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
        .subscribe(notifikacijeServis::posalji);
```

Karakteristike ovog scenarija:

- Podaci **stižu kroz vreme** - svaki tick na berzi je nova emisija.
- Tok **nema kraj** dok je berza otvorena.
- Tempo diktira **berza** (push) - aplikacija samo prati.
- Mora postojati `subscribe()` da bi se išta desilo.
- **Vreme je deo logike** - `sample(5min)` koristi vreme kao
  prvoklasni operator.
- Greška je **signal u toku** (`onError`), ne izuzetak.

To je **push**, asinhrono, scheduler bira nit. Stream ovo **ne ume** -
nema vremensku semantiku, terminalna operacija bi blokirala zauvek.

### Šta uče oba scenarija

Operatori `filter`, `map`, `count` *imaju isto značenje* u oba sveta -
to je pedagoški most. Razlika je u **kontekstu**:

| Pitanje | Stream odgovor | Flux odgovor |
|---------|----------------|---------------|
| Imaju li podaci kraj? | da | ne mora |
| Ko diktira tempo? | terminalna operacija (pull) | izvor (push) sa `request(n)` |
| Kad se izvršava? | čim se pozove terminalna | čim se pozove `subscribe()` |
| Vreme između elemenata | nije pojam | prvoklasni pojam |
| Greška | bačeni izuzetak | signal `onError` |
| Nit | nit poziva | scheduler bira |

### Detaljna tabela razlika

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

## 8. Prednosti i zamke reaktivnog pristupa

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

Svaka zamka ovde dolazi iz neke realne situacije iz produkcije ili sa
prošlih generacija ovog kursa. Pamtite ih sa pričom, ne sa pravilom.

#### Zamka 1: zaboravljen `subscribe()`

```java
http.get("/me")
    .map(this::parsujProfil)
    .doOnNext(this::sacuvajUKes);   // <-- zaboravljen .subscribe()
```

> **Pravilo:** `subscribe()` se *mora pozvati* da bi se išta desilo.
> Mono/Flux su **lenji** - sastavljanje pipeline-a je samo nacrt.

> **Incident:** Junior developer je proveo dva sata debug-ujući zašto
> mu se metoda *ne poziva*. Lambda u `.map(...)` se nikad nije
> izvršila. Rešenje je bilo dodavanje `.subscribe()` na kraju.
> Mono/Flux nisu kao `CompletableFuture` - oni *čekaju* da im neko
> kaže "kreni".

#### Zamka 2: blokiranje u operatoru

```java
flux.map(id -> {
    return restTemplate.getForObject("/users/" + id, User.class);  // SINHRONI HTTP poziv!
});
```

> **Pravilo:** blokirati u operatoru (`Thread.sleep`, sinhroni I/O,
> `block()` unutar `map`) je gotovo uvek bug - blokirate nit
> schedulera koji nije za to namenjen.

> **Incident:** Production server u 3 ujutro počeo da odgovara po 30
> sekundi. Uzrok: neko je stavio sinhroni `restTemplate.getForObject`
> unutar `.map(...)` na `Schedulers.parallel()` - schedulera koji ima
> tačno onoliko niti koliko ima CPU-jezgara. Sve niti su istovremeno
> bile blokirane na sinhrone HTTP pozive. Server *nije bio
> preopterećen* - jednostavno više nije imao slobodne niti za rad.
> Lek: koristi `WebClient` (asinhron) ili prebaci na
> `Schedulers.boundedElastic()` koji je za blokirajuće pozive.

#### Zamka 3: side-effect u `map`

```java
flux.map(user -> {
    posaljiEmail(user);              // <-- side effect ne pripada ovde
    return user.getEmail();
})
.retry(3);                            // <-- ako padne, email ide ponovo
```

> **Pravilo:** side-effects pripadaju u `doOnNext` / `doOnError` /
> `doOnSubscribe`, **ne** u `map`. `map` mora biti **čista funkcija**.

> **Incident:** QA je prijavio da neki korisnici dobiju isti email *četiri
> puta*. Ispostavilo se da je `posaljiEmail` bio u `.map(...)`, a sa
> `.retry(3)` se ceo pipeline ponavljao na grešku - svaki retry je
> ponovo poslao email. Lek: `.doOnNext(this::posaljiEmail).map(User::getEmail)`.
> `doOnNext` *eksplicitno tvrdi* "ovde je side-effect" - i lakše je
> uočiti gde retry boli.

#### Zamka 4: test sa `Thread.sleep`

```java
@Test
void emisija() {
    var rezultati = new ArrayList<>();
    flux.subscribe(rezultati::add);
    Thread.sleep(100);                    // <-- "valjda stigne za 100ms"
    assertEquals(3, rezultati.size());
}
```

> **Pravilo:** test sa `Thread.sleep` je flaky. Koristi `StepVerifier`
> i `VirtualTimeScheduler` (videćemo u nedeljama koje dolaze).

> **Incident:** CI build pukao 1 od 50 puta sa "expected 3, got 2".
> Lokalno je test prošao 100/100 puta. Razlog: na sporom CI runner-u
> 100ms ponekad nije bilo dovoljno - emisije nisu sve stigle pre nego
> što je `assertEquals` izvršen. `StepVerifier.create(flux)
> .expectNextCount(3).verifyComplete()` rešava deterministički, bez
> realnog vremena.

---

## 9. Šta dolazi sledeće - Project Reactor

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

## 10. Primeri koda

| Fajl | Tema |
|------|------|
| [`PushVsPullModel.java`](PushVsPullModel.java) | Pull (Iterator) vs. push (callback / Observer) - konceptualna ilustracija |
| [`ReactiveStreamsSpecification.java`](ReactiveStreamsSpecification.java) | Ručna implementacija `Publisher` / `Subscriber` / `Subscription` (Flow API) |
| [`JavaFlowApiDemo.java`](JavaFlowApiDemo.java) | `SubmissionPublisher` i Flow.Subscriber u akciji - runnable primer |
| [`BlockingVsNonBlocking.java`](BlockingVsNonBlocking.java) | Era 2 vs. Era 4 u brojevima - peak broj niti i vreme za 100 paralelnih poziva |
| [`StreamVsReactive.java`](StreamVsReactive.java) | Isti pipeline kao `Stream<T>` i kao `Flux<T>` - razlika u ponašanju |
| [`Introduction.java`](Introduction.java) | "Hello reactive world" sa Project Reactor-om - sneak peek za nedelju 2 |

---
