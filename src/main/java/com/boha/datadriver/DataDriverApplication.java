package com.boha.datadriver;

import com.boha.datadriver.models.FlatEvent;
import com.boha.datadriver.models.GCSBlob;
import com.boha.datadriver.services.CityService;
import com.boha.datadriver.services.EventSubscriber;
import com.boha.datadriver.services.FirebaseService;
import com.boha.datadriver.services.StorageService;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.SecretMgr;
import com.boha.datadriver.util.Topics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * The DataDriver app hosts the services that enable the creation of a data streaming demo
 */
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

	@Autowired
	private  FirebaseService firebaseService;
	@Autowired
	private Topics topics;
	@Value("${eventTopicId}")
	private String eventTopicId;
	@Autowired
	private EventSubscriber eventSubscriber;

	@Autowired
	private Environment environment;

	@Autowired
	private SecretMgr secrets;
	@Autowired
	private StorageService storageService;

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		String projectId = environment.getProperty("PROJECT_ID");
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " Data Driver ApplicationReadyEvent - Timestamp: "
				+ E.YELLOW_STAR + event.getTimestamp());
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " onApplicationEvent Project: " + projectId + " "
				+ E.YELLOW_STAR );
		LOGGER.info(E.RED_DOT+E.RED_DOT
				+  " Topic: " + eventTopicId  + " " + E.YELLOW_STAR );

		ApplicationContext applicationContext = event.getApplicationContext();
		RequestMappingHandlerMapping requestMappingHandlerMapping = applicationContext
				.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);
		Map<RequestMappingInfo, HandlerMethod> map = requestMappingHandlerMapping
				.getHandlerMethods();
		map.forEach((key, value) -> LOGGER.info(E.PEAR+ " Endpoint: " + key));

		try {
			firebaseService.initializeFirebase();
			String apiKey = secrets.getPlacesAPIKey();
			if (apiKey.contains("AIza")) {
				LOGGER.info(E.CHECK + E.CHECK + E.CHECK +
						" Places API Key starts with AIza and  should be OK .... ");
			}
			int number = 1;
//			List<GCSBlob> list = storageService.listObjects(number);
//			List<FlatEvent> flatEvents  = storageService.getRecentFlatEvents(number);
			topics.printTopicNames();
//			LOGGER.info(E.CHECK + E.CHECK + E.CHECK + "Google Cloud Storage number of blobs in: " +
//					number + " hours: " + E.PEAR+ " " + list.size());
//			String name = list.get(list.size() - 1).getName();
//			String json = storageService.downloadObject(name);
//			LOGGER.info(" Last Blob content: " + json);

//			LOGGER.info(E.RED_APPLE+E.RED_APPLE+" events from GCS: " + flatEvents.size() + " " + E.RED_APPLE);//storageService.init();
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
