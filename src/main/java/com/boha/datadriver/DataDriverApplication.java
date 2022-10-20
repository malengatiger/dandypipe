package com.boha.datadriver;

import com.boha.datadriver.util.E;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

import java.util.logging.Logger;

@SpringBootApplication
public class DataDriverApplication implements ApplicationListener<ApplicationReadyEvent> {

	private static final Logger LOGGER = Logger.getLogger(DataDriverApplication.class.getSimpleName());
	public static void main(String[] args) {

		LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+ " DataDriver starting .....");
		SpringApplication.run(DataDriverApplication.class, args);
		LOGGER.info(
				E.CHECK + E.CHECK +
				" DataDriver completed starting. " + E.CHECK+E.CHECK);
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " Data Driver ApplicationReadyEvent - Timestamp: "
				+ E.YELLOW_STAR + event.getTimestamp());

	}
}
