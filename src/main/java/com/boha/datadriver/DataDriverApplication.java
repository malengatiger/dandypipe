package com.boha.datadriver;

import com.boha.datadriver.services.EventSubscriber;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.SecretMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;

import java.util.logging.Logger;

@SpringBootApplication
public class DataDriverApplication implements ApplicationListener<ApplicationReadyEvent> {

	private static final Logger LOGGER = Logger.getLogger(DataDriverApplication.class.getSimpleName());
	public static void main(String[] args) {

		LOGGER.info(E.BLUE_DOT+E.BLUE_DOT+ " DataDriver starting .....");
		SpringApplication.run(DataDriverApplication.class, args);
		LOGGER.info(
				E.CHECK + E.CHECK +
				" DataDriver completed starting process. " + E.CHECK+E.CHECK);
	}

	@Value("${eventTopicId}")
	private String eventTopicId;
	@Autowired
	private EventSubscriber eventSubscriber;

	@Autowired
	private Environment environment;

	@Autowired
	private SecretMgr secrets;
//	@Autowired
//	private StorageService storageService;
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		String projectId = environment.getProperty("PROJECT_ID");
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " Data Driver ApplicationReadyEvent - Timestamp: "
				+ E.YELLOW_STAR + event.getTimestamp());
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " onApplicationEvent Project: " + projectId + " "
				+ E.YELLOW_STAR );
		LOGGER.info(E.RED_DOT+E.RED_DOT
				+  " Topic: " + eventTopicId  + " " + E.YELLOW_STAR );

		try {
			String apiKey = secrets.getPlacesAPIKey();
			if (apiKey.contains("AIza")) {
				LOGGER.info(E.CHECK + E.CHECK + E.CHECK +
						" Places API Key starts with AIza and  should be OK .... ");
			}
					//storageService.init();
		} catch (Exception e) {
			LOGGER.info(E.RED_DOT + "  We have a problem: " + e.getMessage());
			e.printStackTrace();
		}


	}
//	@Bean
//	ApplicationRunner storageRunner(Storage storage, GcpProjectIdProvider projectIdProvider) {
//		AtomicInteger count = new AtomicInteger();
//		return (args) -> {
//			Page<Blob> list = storage.list(projectIdProvider.getProjectId());
//
//			list.iterateAll().forEach(blob -> {
//				//LOGGER.info(E.BLUE_HEART+E.BLUE_HEART + " " + blob.getName());
//				count.getAndIncrement();
//			});
//			LOGGER.info(E.BLUE_HEART+E.BLUE_HEART + E.RED_APPLE+
//					" " + count.get() + " cloud storage files " + E.RED_APPLE+E.RED_APPLE+E.RED_APPLE);
//		};
//	}
}
