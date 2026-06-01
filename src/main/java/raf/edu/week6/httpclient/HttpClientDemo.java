package raf.edu.week6.httpclient;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Nedelja 6 - reaktivni HTTP klijent (reactor-netty).
 *
 * Koristimo {@link HttpClient} iz reactor-netty paketa - to je donji
 * sloj iznad kojeg je Spring napravio svoj WebClient. Za potrebe
 * predmeta dovoljan je sam HttpClient: jedan zavisni jar, bez Spring-a.
 *
 * Endpoint: https://jsonplaceholder.typicode.com - public mock REST API
 * koji se koristi u tutorialima. Stabilan, vraća JSON, podržava GET/POST.
 *
 * VAŽNO: demo zahteva internet. Bez mreže videće se TimeoutException
 * ili UnknownHostException - to je takođe edukativno (vidi se rad
 * retry/fallback obrazaca).
 */
public class HttpClientDemo {

    private static final DateTimeFormatter HHMMSS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static final String BASE = "https://jsonplaceholder.typicode.com";

    // Jedan deljeni klijent - drži connection pool, NE pravi se po pozivu.
    private static final HttpClient CLIENT = HttpClient.create()
            .responseTimeout(Duration.ofSeconds(5));

    public static void main(String[] args) {
        System.out.println("=== 1. GET - prost poziv, vraća JSON kao String ===\n");
        primer1_prostGet();

        System.out.println("\n=== 2. Paralelni GET-ovi - flatMap nad listom ID-jeva ===\n");
        primer2_paralelniGet();

        System.out.println("\n=== 3. Timeout + retry + fallback - resilience obrazac ===\n");
        primer3_timeoutRetryFallback();

        System.out.println("\n=== 4. POST sa JSON body-jem ===\n");
        primer4_post();
    }

    // -------------------------------------------------------------------
    // GET koji vraća telo kao jedan String.
    //
    // Lanac: responseContent() -> ByteBufFlux (paketi)
    //        aggregate()       -> Mono<ByteBuf> (sklopljeno)
    //        asString()        -> Mono<String>  (UTF-8 dekodirano)
    //
    // block() na kraju jer smo u main metodi - inače bi se main završio
    // pre nego što odgovor stigne.
    // -------------------------------------------------------------------
    static void primer1_prostGet() {
        String json = CLIENT.get()
                .uri(BASE + "/posts/1")
                .responseContent()
                .aggregate()
                .asString()
                .block();

        log("odgovor", skrati(json));
    }

    // -------------------------------------------------------------------
    // Paralelni pozivi: flatMap (nedelja 3) već radi posao - svaki
    // unutrašnji Mono Netty pokreće na svojim event-loop-ovima, ne na
    // posebnim thread-ovima.
    //
    // Bitno: concurrency parametar (drugi argument flatMap-a) kontroliše
    // koliko se istovremenih HTTP-poziva sme imati. Bez njega svi 10
    // krenu odjednom - što jeste paralelno, ali može da zaguši server.
    // -------------------------------------------------------------------
    static void primer2_paralelniGet() {
        long t0 = System.currentTimeMillis();

        List<String> rezultati = Flux.range(1, 5)
                .flatMap(id -> CLIENT.get()
                                .uri(BASE + "/posts/" + id)
                                .responseContent()
                                .aggregate()
                                .asString()
                                .map(body -> "post#" + id + " -> " + skrati(body, 50)),
                        /*concurrency=*/ 5)
                .collectList()
                .block();

        long dt = System.currentTimeMillis() - t0;
        rezultati.forEach(r -> log("paralelno", r));
        log("vreme", dt + "ms (5 poziva uporedo, ne sekvencijalno)");
    }

    // -------------------------------------------------------------------
    // Klasičan resilience obrazac koji smo prethodno modelovali u
    // nedelji 5, sada nad pravim HTTP pozivom.
    //
    // 1) timeout(2s)        - bilo koji poziv koji traje > 2s je fail.
    // 2) retryWhen(backoff) - do 3 pokušaja sa eksponencijalnim
    //                         backoff-om i jitter-om.
    // 3) filter             - retry-jemo SAMO prolazne greške
    //                         (Timeout, IOException). Bug u kodu ne
    //                         treba retry.
    // 4) onErrorReturn      - poslednji bedem: ako svi pokušaji padnu,
    //                         vrati fallback string.
    //
    // Da bismo videli retry, gađamo URL koji vraća 404 i pretvaramo
    // ga u IOException preko responseSingle - on baca exception ako
    // status nije 2xx i mapirali smo ga.
    // -------------------------------------------------------------------
    static void primer3_timeoutRetryFallback() {
        long t0 = System.currentTimeMillis();

        String rez = CLIENT.get()
                .uri(BASE + "/nepostojeci-endpoint-12345")
                .responseSingle((resp, body) -> {
                    if (resp.status().code() >= 400) {
                        return Mono.error(new IOException("HTTP " + resp.status().code()));
                    }
                    return body.asString();
                })
                .timeout(Duration.ofSeconds(2))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
                        .jitter(0.5)
                        .filter(ex -> ex instanceof TimeoutException
                                   || ex instanceof IOException)
                        .doBeforeRetry(rs -> log("retry",
                                "pokušaj " + (rs.totalRetries() + 2)
                                        + " @ " + (System.currentTimeMillis() - t0) + "ms"
                                        + " razlog=" + rs.failure().getClass().getSimpleName())))
                .onErrorReturn("FALLBACK")
                .block();

        log("rezultat", rez + " (ukupno=" + (System.currentTimeMillis() - t0) + "ms)");
    }

    // -------------------------------------------------------------------
    // POST sa JSON body-jem.
    //
    // .post().send(Mono<ByteBuf>) - šaljemo body. Najjednostavnije je
    // dati string i pretvoriti ga u ByteBuf preko ByteBufFlux.fromString.
    //
    // Sa responseSingle dobijamo (response, body) - korisno ako želimo
    // i status kod, ne samo telo.
    // -------------------------------------------------------------------
    static void primer4_post() {
        String json = "{\"title\":\"reaktivno\",\"body\":\"hello\",\"userId\":1}";

        String odgovor = CLIENT
                .headers(h -> h.add("Content-Type", "application/json"))
                .post()
                .uri(BASE + "/posts")
                .send(reactor.netty.ByteBufFlux.fromString(Mono.just(json)))
                .responseSingle((resp, body) ->
                        body.asString()
                                .map(s -> "status=" + resp.status().code() + " body=" + skrati(s, 80)))
                .block();

        log("post-odgovor", odgovor);
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    static String skrati(String s) {
        return skrati(s, 120);
    }

    static String skrati(String s, int max) {
        if (s == null) return "null";
        String oneline = s.replace('\n', ' ').replaceAll("\\s+", " ");
        return oneline.length() > max ? oneline.substring(0, max) + "..." : oneline;
    }

    static void log(String tag, Object value) {
        System.out.printf("  [%s] %-15s on %-22s -> %s%n",
                LocalTime.now().format(HHMMSS),
                tag,
                Thread.currentThread().getName(),
                value);
    }
}
