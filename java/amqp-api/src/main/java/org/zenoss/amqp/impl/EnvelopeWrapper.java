package org.zenoss.amqp.impl;

import org.zenoss.amqp.MessageEnvelope;

import com.rabbitmq.client.Envelope;

/**
 * Wraps a RabbitMQ {@link Envelope} class within a {@link MessageEnvelope}
 * interface.
 */
class EnvelopeWrapper implements MessageEnvelope {

    private final Envelope envelope;

    EnvelopeWrapper(Envelope envelope) {
        this.envelope = envelope;
    }

    @Override
    public long getDeliveryTag() {
        return this.envelope.getDeliveryTag();
    }

    @Override
    public boolean isRedeliver() {
        return this.envelope.isRedeliver();
    }

    @Override
    public String getExchangeName() {
        return this.envelope.getExchange();
    }

    @Override
    public String getRoutingKey() {
        return this.envelope.getRoutingKey();
    }

    @Override
    public String toString() {
        return String
                .format("Envelope [deliveryTag=%s, redeliver=%s, exchangeName=%s, routingKey=%s]",
                        getDeliveryTag(), isRedeliver(), getExchangeName(),
                        getRoutingKey());
    }

}
