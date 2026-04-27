package raf.edu.week1;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

/**
 * Nedelja 1 — Java Flow API u praksi
 *
 * U prethodnom primeru smo ručno implementirali Publisher i Subscription.
 * Java standardna biblioteka već ima gotovog Publisher-a:
 *   {@link java.util.concurrent.SubmissionPublisher}.
 *
 * Ovo je MOST između Stream API-ja (sinhrono) i pravih reaktivnih
 * biblioteka (Reactor, RxJava). Sam Flow API NEMA operatore (map,
 * filter, ...) — zato u nedelji 2 prelazimo na Project Reactor.
 *
 * Demonstrira:
 *  - SubmissionPublisher kao gotov Publisher
 *  - Flow.Subscriber implementacija
 *  - Async dispatch (SubmissionPublisher koristi ForkJoinPool po default-u)
 *  - try-with-resources — close() šalje onComplete
 */
public class JavaFlowApiDemo {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== SubmissionPublisher + Flow.Subscriber ===\n");
        submissionPublisherDemo();

        System.out.println("\n=== Više Subscriber-a na istom Publisher-u ===\n");
        multiSubscriberDemo();
    }

    // -----------------------------------------------------------------------
    // Osnovni demo — jedan Publisher, jedan Subscriber.
    // SubmissionPublisher dispatcuje na ForkJoinPool — zato se
    // ime niti razlikuje od main niti.
    // -----------------------------------------------------------------------
    static void submissionPublisherDemo() throws InterruptedException {
        // try-with-resources: close() šalje onComplete svim Subscriber-ima.
        try (SubmissionPublisher<String> publisher = new SubmissionPublisher<>()) {
            publisher.subscribe(new PrintingSubscriber("S1"));

            List.of("Beograd", "Niš", "Novi Sad", "Kragujevac")
                    .forEach(publisher::submit);

            // Sačekaj malo — submit je asinhron, ne želimo da main istekne pre dispatch-a.
            // U realnom sistemu se ne čeka ovako; ovo je samo zato što je demo.
            Thread.sleep(300);
        }
        // close() → onComplete; sleep da stigne signal pre nego što main izađe.
        Thread.sleep(100);
    }

    // -----------------------------------------------------------------------
    // Više Subscriber-a — svaki dobija KOPIJU svake emisije.
    // Ovo je tzv. "hot" multicast — emitovani element ide svima koji su
    // u tom trenutku pretplaćeni. (Detaljnije o hot/cold u nedelji 7.)
    // -----------------------------------------------------------------------
    static void multiSubscriberDemo() throws InterruptedException {
        try (SubmissionPublisher<Integer> publisher = new SubmissionPublisher<>()) {
            publisher.subscribe(new PrintingSubscriber("Brojač A"));
            publisher.subscribe(new PrintingSubscriber("Brojač B"));

            for (int i = 1; i <= 5; i++) {
                publisher.submit(i);
            }

            // Daj malo vremena async dispatcher-u
            publisher.consume(_ -> { /* ignored — već imamo subscriber */ })
                    .get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // demo ignoriše
        }
        Thread.sleep(100);
    }

    // -----------------------------------------------------------------------
    // Jednostavan Subscriber koji štampa sve signale i traži po 1 element.
    // Tražimo Long.MAX_VALUE — nema backpressure-a — što je OK za demo,
    // ali nije pristup za production.
    // -----------------------------------------------------------------------
    static class PrintingSubscriber implements Flow.Subscriber<Object> {
        private final String name;
        private Flow.Subscription subscription;

        PrintingSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            System.out.println("  [" + name + "] onSubscribe → request(MAX)");
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Object item) {
            System.out.println("  [" + name + " @ " + Thread.currentThread().getName()
                    + "] onNext: " + item);
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("  [" + name + "] onError: " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            System.out.println("  [" + name + "] onComplete");
        }
    }
}
