package com.knippqai.sample;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.thoughtworks.gauge.AfterScenario;
import com.thoughtworks.gauge.BeforeScenario;
import com.thoughtworks.gauge.Step;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dasselbe Acceptance-Modell wie das gauge-ts-Referenzprojekt: echte
 * Order-Demo in der Plattformumgebung, deterministische Adapter-Mocks in CI.
 * KnippQAi-spezifische Hooks oder Dependencies enthält das Projekt bewusst nicht.
 */
public class OrderSteps {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final MediaType JSON_MEDIA = MediaType.get("application/json");

    private final boolean mock = "mock".equalsIgnoreCase(env("ORDER_TEST_MODE", "mock"));
    private final String api = env("ORDER_API_URL", "http://localhost:18081");
    private final String ui = env("ORDER_UI_URL", "http://localhost:18080");
    private final String brokers = env("KAFKA_BROKERS", "localhost:19092");
    private final OkHttpClient http = new OkHttpClient();

    private MockWebServer mockApi;
    private Playwright playwright;
    private Browser browser;
    private Page page;
    private MockProducer<String, String> mockProducer;
    private JsonNode order;
    private int responseStatus;

    @BeforeScenario
    public void prepareAdapters() throws Exception {
        if (!mock) return;
        mockApi = new MockWebServer();
        mockApi.start();
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @AfterScenario
    public void closeAdapters() throws Exception {
        try { if (browser != null) browser.close(); } finally { browser = null; page = null; }
        try { if (playwright != null) playwright.close(); } finally { playwright = null; }
        try { if (mockProducer != null) mockProducer.close(); } finally { mockProducer = null; }
        try { if (mockApi != null) mockApi.close(); } finally { mockApi = null; }
    }

    @Step("The trader opens the order entry page")
    public void openOrderEntry() {
        playwright = Playwright.create();
        browser = playwright.chromium().launch();
        page = browser.newPage();
        if (mock) page.setContent(mockOrderDesk());
        else page.navigate(ui);
    }

    @Step("The trader submits a <side> order for <quantity> <instrument> at <price> EUR through the UI")
    public void submitThroughUi(String side, int quantity, String instrument, double price) {
        page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
            new Page.GetByRoleOptions().setName("New order")).click();
        page.locator("select[name=instrument]").selectOption(instrument);
        page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
            new Page.GetByRoleOptions().setName(side).setExact(true)).click();
        page.locator("input[name=quantity]").fill(String.valueOf(quantity));
        page.locator("input[name=limitPrice]").fill(String.valueOf(price));
        page.getByRole(com.microsoft.playwright.options.AriaRole.BUTTON,
            new Page.GetByRoleOptions().setName("Submit order")).click();
    }

    @Step("The order blotter shows <instrument> with status <status>")
    public void blotterShows(String instrument, String status) {
        var row = page.locator("#orders tr", new Page.LocatorOptions().setHasText(instrument)).first();
        row.waitFor();
        assertEquals(status, row.getAttribute("data-status"));
    }

    @Step("Customer <customer> submits a limit <side> order for <quantity> <instrument> at <price> EUR through REST")
    public void submitThroughRest(String customer, String side, int quantity, String instrument, double price) throws Exception {
        var payload = JSON.writeValueAsString(Map.of(
            "customerId", customer, "side", side, "quantity", quantity,
            "instrument", instrument, "limitPrice", price));
        if (mock) {
            var id = "mock-" + UUID.randomUUID();
            mockApi.enqueue(new MockResponse().setResponseCode(201).addHeader("content-type", "application/json")
                .setBody(JSON.writeValueAsString(Map.of("orderId", id, "status", "NEW"))));
        }
        var endpoint = mock ? mockApi.url("/orders").toString() : api + "/orders";
        var request = new Request.Builder().url(endpoint).post(RequestBody.create(payload, JSON_MEDIA)).build();
        try (var response = http.newCall(request).execute()) {
            responseStatus = response.code();
            assertNotNull(response.body());
            order = JSON.readTree(response.body().string());
        }
    }

    @Step("The order command API accepts the order with status <status>")
    public void apiAccepts(String status) {
        assertEquals(201, responseStatus);
        assertEquals(status, order.path("status").asText());
        assertTrue(order.path("orderId").isTextual());
    }

    @Step("An order-created event exists for the order on topic <topic>")
    public void orderCreatedEvent(String topic) throws Exception {
        var orderId = order.path("orderId").asText();
        if (mock) {
            mockProducer.send(new ProducerRecord<>(topic, orderId, "ORDER_CREATED")).get();
            assertEquals(orderId, mockProducer.history().getFirst().key());
            return;
        }
        var properties = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
            ConsumerConfig.GROUP_ID_CONFIG, "gauge-java-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (var consumer = new KafkaConsumer<String, byte[]>(properties)) {
            consumer.subscribe(List.of(topic));
            var deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(300)).records(topic)) {
                    if (orderId.equals(record.key()) && record.value() != null
                        && record.value().length > 5 && record.value()[0] == 0) return;
                }
            }
        }
        throw new AssertionError("No Confluent-Avro order event for " + orderId + " on " + topic);
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, System.getProperty(name, fallback));
    }

    private static String mockOrderDesk() {
        return """
            <button id='newOrder'>New order</button>
            <form id='orderForm' hidden>
              <select name='instrument'><option>SAP</option><option>SIE</option></select>
              <button type='button' data-side='BUY'>BUY</button><button type='button' data-side='SELL'>SELL</button>
              <input name='side' type='hidden' value='BUY'><input name='quantity'><input name='limitPrice'>
              <button type='submit'>Submit order</button>
            </form><table><tbody id='orders'></tbody></table>
            <script>
              newOrder.onclick=()=>orderForm.hidden=false;
              document.querySelectorAll('[data-side]').forEach(b=>b.onclick=()=>document.querySelector('[name=side]').value=b.dataset.side);
              orderForm.onsubmit=e=>{e.preventDefault();const d=Object.fromEntries(new FormData(e.target));orders.innerHTML=`<tr data-status='NEW'><td>${d.instrument}</td><td>NEW</td></tr>`};
            </script>
            """;
    }
}
