package com.boha.datadriver;

import com.boha.datadriver.services.EventSubscriber;
import com.boha.datadriver.services.FirebaseService;
import com.boha.datadriver.services.StorageService;
import com.boha.datadriver.util.E;
import com.boha.datadriver.util.SecretMgr;
import com.boha.datadriver.util.Topics;
import com.boha.datadriver.util.WriteLogEntry;
import com.google.firebase.FirebaseApp;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
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
	private WriteLogEntry writeLogEntry;
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

	FirebaseApp app;
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		String projectId = environment.getProperty("PROJECT_ID");
		app = firebaseService.initializeFirebase();

		String address = null;
		try {
			address = InetAddress.getLocalHost().getHostAddress();
			LOGGER.info(E.RED_DOT+E.RED_DOT+" server address: " + address);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}


				LOGGER.info(E.RED_DOT+E.RED_DOT+ " Data Driver ApplicationReadyEvent - Timestamp: "
				+ E.YELLOW_STAR + event.getTimestamp());
		LOGGER.info(E.RED_DOT+E.RED_DOT+ " onApplicationEvent Project: " + projectId + " "
				+ E.YELLOW_STAR );
		LOGGER.info(E.RED_DOT+E.RED_DOT
				+  " Topic: " + eventTopicId  + " " + E.YELLOW_STAR );
		//writeLogEntry.info("Data Driver: Things have started OK this far: " + DateTime.now().toDateTimeISO().toString());

		ApplicationContext applicationContext = event.getApplicationContext();
		RequestMappingHandlerMapping requestMappingHandlerMapping = applicationContext
				.getBean("requestMappingHandlerMapping", RequestMappingHandlerMapping.class);
		Map<RequestMappingInfo, HandlerMethod> map = requestMappingHandlerMapping
				.getHandlerMethods();
		map.forEach((key, value) -> {
			LOGGER.info(E.PEAR+E.PEAR+
					" Endpoint: " + key);
		});

		try {

			String apiKey = secrets.getPlacesAPIKey();
			if (apiKey.contains("AIza")) {
				LOGGER.info(E.CHECK + E.CHECK + E.CHECK +
						" Places API Key starts with AIza and  should be OK .... ");
			}
			topics.printTopicNames();
		} catch (Exception e) {
			LOGGER.info(E.RED_DOT + "  We have a problem: " + e.getMessage());
			e.printStackTrace();
		}


	}
	@Bean
	public CorsConfigurationSource corsConfigurationSource() {
		final CorsConfiguration config = new CorsConfiguration();

		config.setAllowedOrigins(List.of("http://localhost:4200"));
		config.setAllowedMethods(Arrays.asList("GET", "POST", "OPTIONS", "DELETE", "PUT", "PATCH"));
		config.setAllowCredentials(true);
		config.setAllowedHeaders(Arrays.asList("Authorization", "Cache-Control", "Content-Type"));

		final UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
		source.registerCorsConfiguration("/**", config);

		return source;
	}
}
