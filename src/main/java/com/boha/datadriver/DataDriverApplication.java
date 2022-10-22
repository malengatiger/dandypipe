package com.boha.datadriver;

import com.boha.datadriver.services.EventSubscriber;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.Secrets;
import com.google.api.gax.paging.Page;
import com.google.cloud.spring.core.GcpProjectIdProvider;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicInteger;
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
	@Value("${projectId}")
	private String projectId;
	@Value("${topicId}")
	private String topicId;
	@Autowired
	private EventSubscriber eventSubscriber;
	@Autowired
	private Secrets secrets;
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " Data Driver ApplicationReadyEvent - Timestamp: "
				+ E.YELLOW_STAR + event.getTimestamp());
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " Project: " + projectId + " "
				+ E.YELLOW_STAR );
		LOGGER.info(E.RED_DOT+E.RED_DOT
				+  " Topic: " + topicId  + " " + E.YELLOW_STAR );

		try {
			String apiKey = secrets.getSecret();
			LOGGER.info(E.RED_DOT+E.RED_DOT
					+  " ApiKey?: " + apiKey  + " " + E.YELLOW_STAR );
		} catch (Exception e) {
			throw new RuntimeException(e);
		}


	}
	@Bean
	ApplicationRunner storageRunner(Storage storage, GcpProjectIdProvider projectIdProvider) {
		AtomicInteger count = new AtomicInteger();
		return (args) -> {
			Page<Blob> list = storage.list(projectIdProvider.getProjectId());

			list.iterateAll().forEach(blob -> {
				LOGGER.info(E.BLUE_HEART+E.BLUE_HEART + " " + blob.getName());
				count.getAndIncrement();
			});
			LOGGER.info(E.BLUE_HEART+E.BLUE_HEART + E.RED_APPLE+
					" " + count.get() + " cloud storage files " + E.RED_APPLE+E.RED_APPLE+E.RED_APPLE);
		};
	}
}
