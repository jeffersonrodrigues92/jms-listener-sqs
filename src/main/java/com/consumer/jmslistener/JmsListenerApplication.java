package com.consumer.jmslistener;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jms.annotation.EnableJms;

@SpringBootApplication
@EnableJms
public class JmsListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(JmsListenerApplication.class, args);
	}

}
