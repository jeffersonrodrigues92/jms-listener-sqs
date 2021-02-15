package com.consumer.jmslistener.component;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class ConsumerListener {

    @JmsListener(destination = "teste-apolo")
    public void messageConsumer(@Headers Map<String, Object> messageAttributes, @Payload String message) throws Exception {
        System.out.println("Messages attributes: " + messageAttributes);
        System.out.println("Body: " + message);
        throw new Exception("deu ruim");
    }

}