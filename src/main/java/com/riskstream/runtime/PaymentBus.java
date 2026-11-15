package com.riskstream.runtime;

import com.riskstream.model.Payment;

/** Where payments enter the stream. Implemented once for Kafka and once for the embedded runtime. */
public interface PaymentBus {

    void publish(Payment payment);

    /** One line for the console saying what is running underneath. */
    String describe();
}
