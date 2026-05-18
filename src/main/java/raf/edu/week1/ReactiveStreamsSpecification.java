package raf.edu.week1;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nedelja 1 - Reactive Streams specifikacija (Java Flow API)
 *
 * Ručna implementacija Publisher-a i Subscriber-a, BEZ ikakve biblioteke.
 * Cilj: razumeti tačan protokol signala i ulogu Subscription-a.
 *
 * Lifecycle (skraćeno):
 *   onSubscribe(Subscription)            ← uvek prvi
 *      ↓
 *   onNext(T)*                            ← 0 ili više puta, po request-u
 *      ↓
 *   onComplete()  XOR  onError(Throwable) ← terminalni signal
 *
 * Backpressure: Subscriber preko Subscription-a poziva request(n) i
 * Publisher sme da emituje najviše n elemenata pre nego što stigne
 * sledeći request. Tako spor potrošač ne biva preplavljen.
 *
 * Napomena: ovo je MINIMALAN primer za predavanje. Pravi Publisher
 * mora poštovati ~30 pravila iz Reactive Streams specifikacije
 * (thread-safety Subscription-a, idempotentnost cancel-a, itd).
 */
public class ReactiveStreamsSpecification {

    public static void main(String[] args) {
        System.out.println("=== Custom Publisher + Custom Subscriber (Java Flow) ===\n");

        Flow.Publisher<String> publisher = new IterablePublisher<>(
                List.of("Beograd", "Niš", "Novi Sad", "Kragujevac")
        );

        publisher.subscribe(new LoggingSubscriber<>(2));   // request po 2 elementa

        System.out.println("\n=== Posle prvog dela - Subscriber je tražio još ===");
        // Vidi se u logu kako request kontroliše tempo.
    }

    // -----------------------------------------------------------------------
    // IterablePublisher - emituje elemente iz Iterable, jedan po jedan,
    // poštujući request(n).
    // -----------------------------------------------------------------------
    static class IterablePublisher<T> implements Flow.Publisher<T> {
        private final Iterable<T> izvor;

        IterablePublisher(Iterable<T> izvor) {
            this.izvor = izvor;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
            // Spec: prvi signal je uvek onSubscribe.
            subscriber.onSubscribe(new IterableSubscription<>(izvor.iterator(), subscriber));
        }
    }

    // -----------------------------------------------------------------------
    // IterableSubscription - drži stanje (gde smo stali) i implementira
    // request / cancel.
    // -----------------------------------------------------------------------
    static class IterableSubscription<T> implements Flow.Subscription {
        private final java.util.Iterator<T> it;
        private final Flow.Subscriber<? super T> subscriber;
        private final AtomicLong requested = new AtomicLong(0);
        private volatile boolean canceled = false;

        IterableSubscription(java.util.Iterator<T> it, Flow.Subscriber<? super T> subscriber) {
            this.it = it;
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException(
                        "request(n) sa n <= 0 je zabranjen - Reactive Streams pravilo 3.9"));
                return;
            }
            // Dodaj n u "kredit" i pokušaj da emituješ koliko god je slobodno.
            // Pojednostavljeno: ovo nije thread-safe re-entrant pump.
            requested.addAndGet(n);
            drain();
        }

        @Override
        public void cancel() {
            canceled = true;
        }

        private void drain() {
            while (!canceled && requested.get() > 0 && it.hasNext()) {
                T value = it.next();
                requested.decrementAndGet();
                subscriber.onNext(value);
            }
            if (!canceled && !it.hasNext()) {
                // Spec: posle terminalnog signala - niko ne sme više ništa.
                canceled = true;
                subscriber.onComplete();
            }
        }
    }

    // -----------------------------------------------------------------------
    // LoggingSubscriber - prima signale, loguje, i traži dalje po batch-evima.
    // Ovo je KLJUČNI deo backpressure-a - Subscriber sam diktira koliko
    // je spreman da primi.
    // -----------------------------------------------------------------------
    static class LoggingSubscriber<T> implements Flow.Subscriber<T> {
        private final int batchSize;
        private Flow.Subscription subscription;
        private int receivedInBatch = 0;
        private int totalReceived = 0;

        LoggingSubscriber(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            System.out.println("  [onSubscribe] kreće protok - tražim prvi batch od " + batchSize);
            subscription.request(batchSize);
        }

        @Override
        public void onNext(T item) {
            totalReceived++;
            receivedInBatch++;
            System.out.println("  [onNext] #" + totalReceived + " = " + item);

            if (receivedInBatch == batchSize) {
                receivedInBatch = 0;
                System.out.println("  [Subscriber] batch završen, tražim sledećih " + batchSize);
                subscription.request(batchSize);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("  [onError] " + throwable.getMessage());
        }

        @Override
        public void onComplete() {
            System.out.println("  [onComplete] tok je završen, ukupno primljeno: " + totalReceived);
        }
    }
}
