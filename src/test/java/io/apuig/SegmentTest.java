package io.apuig;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.segment.analytics.kotlin.core.Analytics;
import com.segment.analytics.kotlin.core.Configuration;
import com.segment.analytics.kotlin.core.RequestFactory;
import com.segment.analytics.kotlin.core.Storage;
import com.segment.analytics.kotlin.core.StorageProvider;
import com.segment.analytics.kotlin.core.Telemetry;
import com.segment.analytics.kotlin.core.compat.Builders;
import com.segment.analytics.kotlin.core.compat.ConfigurationBuilder;
import com.segment.analytics.kotlin.core.compat.JavaAnalytics;
import com.segment.analytics.kotlin.core.utilities.FileEventStream;
import com.segment.analytics.kotlin.core.utilities.PropertiesFile;
import com.segment.analytics.kotlin.core.utilities.StorageImpl;
import java.io.File;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kotlin.Unit;
import kotlin.coroutines.Continuation;
import kotlinx.coroutines.CoroutineDispatcher;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import sovran.kotlin.Store;
import wiremock.com.fasterxml.jackson.core.JsonProcessingException;
import wiremock.com.fasterxml.jackson.databind.JsonNode;
import wiremock.com.fasterxml.jackson.databind.ObjectMapper;
import wiremock.com.google.common.util.concurrent.RateLimiter;
import wiremock.org.apache.commons.io.FileUtils;
import wiremock.org.apache.commons.lang3.RandomStringUtils;

public class SegmentTest {

    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
            .options(wireMockConfig().port(8088))
            .configureStaticDsl(true)
            .failOnUnmatchedRequests(false)
            .build();

    JavaAnalytics createAnalytics() {

        Telemetry.INSTANCE.setEnable(false);

        Configuration conf = new ConfigurationBuilder("writeKey")
                .setApplication(this)
                .setFlushAt(20)
                .setFlushInterval(30)
                .setApiHost("localhost:8088/v1")
                .setCollectDeviceId(false)
                .setTrackApplicationLifecycleEvents(false)
                .setTrackDeepLinks(false)
                .setUseLifecycleObserver(false)
                .setRequestFactory(new RequestFactory() {
                    @Override
                    public HttpURLConnection openConnection(String url) {
                        return super.openConnection(url.replace("https", "http"));
                    }
                })
                .build();

        conf.setStorageProvider(new StorageProvider() {
            @Override
            public Storage createStorage(Object... params) {
                Analytics analytics = (Analytics) params[0];
                Configuration config = analytics.getConfiguration();
                String writeKey = config.getWriteKey();
                File directory = new File("/tmp/analytics-kotlin/" + writeKey);
                File eventDirectory = new File(directory, "events");
                String fileIndexKey = "segment.events.file.index." + writeKey;
                File userPrefs = new File(directory, "analytics-kotlin-" + writeKey + ".properties");
                PropertiesFile propertiesFile = new PropertiesFile(userPrefs);
                FileEventStream eventStream = new FileEventStream(eventDirectory);
                return new StorageImpl(
                        propertiesFile,
                        eventStream,
                        analytics.getStore(),
                        config.getWriteKey(),
                        fileIndexKey,
                        analytics.getFileIODispatcher()) {
                    @Override
                    public Object write(Constants key, String value, Continuation<? super Unit> $completion) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                        }
                        return super.write(key, value, $completion);
                    }

                    @Override
                    public String read(Constants arg0) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                        }
                        return super.read(arg0);
                    }
                };
            }

            @Override
            public Storage getStorage(
                    com.segment.analytics.kotlin.core.Analytics arg0,
                    Store arg1,
                    String arg2,
                    CoroutineDispatcher arg3,
                    Object arg4) {
                throw new IllegalStateException("expect createStorage");
            }
        });

        return new JavaAnalytics(conf);
    }

    @Test
    public void httpDownAndThenUpAgain() throws Throwable {

        FileUtils.deleteDirectory(Path.of("/tmp/analytics-kotlin/writeKey").toFile());

        JavaAnalytics analytics = createAnalytics();

        int requestsPerSecond = 1_000;
        int numClients = 10;

        int timeToRun = 60_000 * 5;
        int timeToRestore = 60_000 * 4;

        int httpResponseDelay = 1_000;

        System.err.println("failing http request during : " + timeToRestore);

        stubFor(post(urlEqualTo("/v1/b"))
                .willReturn(
                        WireMock.aResponse().withStatus(503).withBody("fail").withFixedDelay(httpResponseDelay)));

        RateLimiter rate = RateLimiter.create(requestsPerSecond);
        ExecutorService exec = new ThreadPoolExecutor(
                numClients, numClients, 15l, TimeUnit.SECONDS, new LinkedBlockingDeque<>(1_000), new AbortPolicy());

        long start = System.currentTimeMillis();
        boolean upAgain = false;
        final AtomicInteger id = new AtomicInteger(1);

        while (System.currentTimeMillis() - start < timeToRun) {
            if (rate.tryAcquire()) {
                exec.submit(() -> {
                    analytics.track("track", Builders.buildJsonObject(o -> {
                        o.put("msgId", id.getAndIncrement())
                                .put("content", RandomStringUtils.randomAlphanumeric(10_000));
                    }));
                });
            }
            if (!upAgain && System.currentTimeMillis() - start > timeToRestore) {
                upAgain = true;
                stubFor(post(urlEqualTo("/v1/b"))
                        .willReturn(okJson("{\"success\": \"true\"}").withFixedDelay(httpResponseDelay)));
                System.err.println("http request now ok, remaining time : " + (timeToRun - timeToRestore));
            }
        }

        int requests = id.get() - 1;
        System.err.println(requests + " requests submited. awaiting");

        Awaitility.await()
                .atMost(10, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> {
                    int delivered = countDeliveredMessages();
                    System.err.println((requests - delivered) + " pending");
                    return requests <= delivered;
                });

        exec.shutdownNow();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static final ObjectMapper OM = new ObjectMapper();

    private int countDeliveredMessages() {
        int count = 0;
        for (ServeEvent event : wireMock.getAllServeEvents()) {
            if (event.getResponse().getStatus() != 200) {
                continue;
            }

            JsonNode batch;
            try {
                JsonNode json = OM.readTree(event.getRequest().getBodyAsString());
                batch = json.get("batch");
                if (batch == null) {
                    continue;
                }
            } catch (JsonProcessingException e) {
                continue;
            }
            Iterator<JsonNode> msgs = batch.elements();
            while (msgs.hasNext()) {
                msgs.next();
                count++;
            }
        }
        return count;
    }
}
