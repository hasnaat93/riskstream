package com.riskstream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * The whole application in embedded mode, through its HTTP API. The traffic generator is off,
 * so the only payments in the stream are the ones each test sends.
 */
@SpringBootTest(properties = {
        "riskstream.mode=embedded",
        "riskstream.simulator.payments-per-second=0",
        "riskstream.simulator.incident-every-seconds=0"
})
@AutoConfigureMockMvc
class ApplicationIT {

    @Autowired
    private MockMvc mvc;

    private record Response(int status, DocumentContext json) {
        <T> T read(String path) {
            return json.read(path);
        }
    }

    private Response send(MockHttpServletRequestBuilder request, String body) throws Exception {
        if (body != null) {
            request.contentType(MediaType.APPLICATION_JSON).content(body);
        }
        MvcResult result = mvc.perform(request).andReturn();
        String text = result.getResponse().getContentAsString();
        boolean isJson = text.trim().startsWith("{") || text.trim().startsWith("[");
        return new Response(result.getResponse().getStatus(), JsonPath.parse(isJson ? text : "{}"));
    }

    private static String payment(String id, String card, long amountMinor, String country, Instant at) {
        return """
                {"paymentId":"%s","cardId":"%s","merchantId":"merchant-1","amountMinor":%d,"currency":"USD",
                 "country":"%s","channel":"ECOMMERCE","occurredAt":"%s"}""".formatted(id, card, amountMinor, country, at);
    }

    @Test
    void everyScenarioRaisesTheAlertItShouldAndTheScoreboardCountsIt() throws Exception {
        for (String[] pair : new String[][] {{"CARD_TESTING", "VELOCITY"}, {"ACCOUNT_TAKEOVER", "GEO_JUMP"},
                                             {"BUST_OUT", "SPEND_LIMIT"}, {"AMOUNT_SPIKE", "AMOUNT_SPIKE"}}) {
            Response injected = send(post("/api/v1/simulate/" + pair[0]), null);
            assertThat(injected.status()).isEqualTo(200);
            assertThat(injected.<String>read("$.expectedRule")).isEqualTo(pair[1]);
            assertThat(injected.<Integer>read("$.payments")).isGreaterThan(1);

            // embedded mode is synchronous: the alert is already there
            Response alerts = send(get("/api/v1/alerts?limit=5&rule=" + pair[1]), null);
            assertThat(alerts.<Integer>read("$.length()")).as(pair[0]).isGreaterThanOrEqualTo(1);
            assertThat(alerts.<String>read("$[0].rule")).isEqualTo(pair[1]);
            assertThat(alerts.<String>read("$[0].summary")).isNotBlank();
        }

        Response stats = send(get("/api/v1/stats"), null);
        assertThat(stats.status()).isEqualTo(200);
        assertThat(stats.<String>read("$.runtime")).startsWith("Embedded");
        assertThat(stats.<List<Integer>>read("$.scenarios[*].injected")).containsExactly(1, 1, 1, 1);
        assertThat(stats.<List<Integer>>read("$.scenarios[*].detected")).containsExactly(1, 1, 1, 1);
        assertThat(stats.<Integer>read("$.processed")).isGreaterThan(30);
        assertThat(stats.<Integer>read("$.alertsByRule.VELOCITY")).isGreaterThanOrEqualTo(1);
        assertThat(stats.<Integer>read("$.late")).isZero();

        assertThat(send(post("/api/v1/simulate/NORMAL"), null).<String>read("$.errorCode")).isEqualTo("NOT_AN_INCIDENT");
        assertThat(send(post("/api/v1/simulate/EARTHQUAKE"), null).status()).isEqualTo(400);
    }

    @Test
    void paymentsPostedOneByOneAreJudgedLikeAnyOthers() throws Exception {
        String card = "card-" + UUID.randomUUID();
        Instant now = Instant.now();
        Response first = send(post("/api/v1/payments"), payment("api-1-" + card, card, 2_000, "PK", now.minusSeconds(600)));
        assertThat(first.status()).isEqualTo(202);
        assertThat(first.<String>read("$.status")).isEqualTo("published");

        send(post("/api/v1/payments"), payment("api-2-" + card, card, 2_500, "BR", now.minusSeconds(60)));
        send(post("/api/v1/payments"), payment("api-2-" + card, card, 2_500, "BR", now.minusSeconds(60)));      // a retry

        Response alerts = send(get("/api/v1/alerts?rule=GEO_JUMP&limit=1"), null);
        assertThat(alerts.<String>read("$[0].cardId")).isEqualTo(card);
        assertThat(alerts.<String>read("$[0].paymentId")).isEqualTo("api-2-" + card);
        assertThat(alerts.<String>read("$[0].summary")).isEqualTo("Used in PK and BR 9 min apart");
        assertThat(alerts.<String>read("$[0].evidence.otherCountry")).isEqualTo("PK");
        assertThat(send(get("/api/v1/stats"), null).<Integer>read("$.duplicates")).isGreaterThanOrEqualTo(1);
    }

    @Test
    void invalidPaymentsAreRejectedBeforeTheyReachTheStream() throws Exception {
        Instant now = Instant.now();
        String good = payment("v-1", "card-v", 1_000, "US", now);
        assertThat(send(post("/api/v1/payments"), good.replace("1000", "0")).<String>read("$.errorCode")).isEqualTo("VALIDATION_FAILED");
        assertThat(send(post("/api/v1/payments"), good.replace("\"US\"", "\"usa\"")).status()).isEqualTo(400);
        assertThat(send(post("/api/v1/payments"), good.replace("\"card-v\"", "\"\"")).status()).isEqualTo(400);
        assertThat(send(post("/api/v1/payments"), good.replace("merchantId", "cardNumber")).<String>read("$.errorCode"))
                .as("an unknown field such as a card number is refused, not ignored").isEqualTo("MALFORMED_REQUEST");
        Response future = send(post("/api/v1/payments"), payment("v-2", "card-v", 1_000, "US", now.plusSeconds(3_600)));
        assertThat(future.status()).isEqualTo(422);
        assertThat(future.<String>read("$.errorCode")).isEqualTo("FUTURE_PAYMENT");
        assertThat(send(get("/api/v1/alerts?limit=0"), null).status()).isEqualTo(400);
    }

    @Test
    void rulesAreDescribedWithTheThresholdsTheyRunWith() throws Exception {
        Response rules = send(get("/api/v1/rules"), null);
        assertThat(rules.<List<String>>read("$[*].rule")).containsExactly("VELOCITY", "SPEND_LIMIT", "GEO_JUMP", "AMOUNT_SPIKE");
        assertThat(rules.<String>read("$[0].threshold")).isEqualTo("More than 5 payments within 60 s");
        assertThat(rules.<String>read("$[1].threshold")).isEqualTo("More than 2,500.00 within 10 min");
    }

    @Test
    void theConsoleApiDocsAndHealthAreServed() throws Exception {
        assertThat(mvc.perform(get("/index.html")).andReturn().getResponse().getContentAsString()).contains("Riskstream");
        assertThat(send(get("/v3/api-docs"), null).<String>read("$.info.title")).isNotBlank();
        assertThat(send(get("/actuator/health"), null).<String>read("$.status")).isEqualTo("UP");
        assertThat(send(get("/api/v1/nowhere"), null).status()).isEqualTo(404);
    }
}
