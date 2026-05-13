package com.trafficmorph.capture.servlet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trafficmorph.capture.CaptureLogger;
import com.trafficmorph.capture.ListSink;
import com.trafficmorph.capture.RedactionPolicy;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.Test;

/**
 * Lightweight end-to-end integration test. The mock-based suite
 * pins spec-contract correctness against a hand-built {@code
 * HttpServletRequest} matrix; this test boots a real Jetty 12
 * servlet container, wires {@link CaptureServletFilter} in front
 * of a tiny echo servlet, and dispatches a real HTTP request
 * over a socket. Catches regressions mocks structurally can't —
 * the real container constructs its own request objects,
 * applies its own header / encoding parsing, drives its own
 * chain dispatch, and exercises the actual jakarta.servlet 6
 * surface.
 *
 * <p>One happy-path test today; deliberately not a sprawling
 * suite — the mocks already cover edge semantics in isolation,
 * and an embedded container is most valuable as a sanity check
 * that the wiring works end-to-end. Future regressions in
 * container-coupled behaviour can land here as additional
 * tests against the same fixture.
 */
class CaptureServletFilterIntegrationTest {

    /**
     * Trivial servlet that reads the request body and writes it
     * back. Used to verify that the filter doesn't disturb body
     * forwarding — what the client sends should match what the
     * servlet receives via {@code req.getInputStream()}.
     */
    private static final class EchoServlet extends HttpServlet {
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp)
                throws IOException {
            try (InputStream in = req.getInputStream()) {
                byte[] body = in.readAllBytes();
                resp.setStatus(200);
                resp.setContentType("application/json");
                resp.getOutputStream().write(body);
            }
        }
    }

    @Test
    void captureFilterEndToEndWithEmbeddedJettyPreservesBodyAndEmitsCaptureLine() throws Exception {
        ListSink sink = new ListSink();
        try (CaptureLogger logger = CaptureLogger.builder()
                .sink(sink)
                .headerRedaction(RedactionPolicy.none())
                .build()) {

            // Stand up Jetty on an ephemeral port.
            Server server = new Server();
            ServerConnector connector = new ServerConnector(server);
            connector.setPort(0);   // OS picks a free port
            server.addConnector(connector);

            ServletContextHandler ctx = new ServletContextHandler();
            ctx.setContextPath("/");
            ctx.addServlet(new ServletHolder(new EchoServlet()), "/echo");

            // Wire the capture filter in front of EVERY request.
            // captureBody=true so we exercise the wrapper path
            // (the integration-interesting code), with a generous
            // cap that easily fits the test payload.
            CaptureServletFilter filter = new CaptureServletFilter(
                    logger, /*captureBody*/ true, /*maxBodyBytes*/ 1024);
            ctx.getServletHandler().addFilterWithMapping(
                    new org.eclipse.jetty.ee10.servlet.FilterHolder(filter),
                    "/*",
                    EnumSet.of(DispatcherType.REQUEST));

            server.setHandler(ctx);
            server.start();
            try {
                int port = connector.getLocalPort();
                String body = "{\"id\":\"x-1\",\"action\":\"bid\"}";

                // Real HTTP exchange over loopback via JDK HttpClient.
                HttpClient client = HttpClient.newHttpClient();
                HttpResponse<String> resp = client.send(
                        HttpRequest.newBuilder()
                                .uri(URI.create("http://localhost:" + port + "/echo?trace=abc"))
                                .header("Content-Type", "application/json")
                                .header("X-Trace-Id", "req-42")
                                .POST(HttpRequest.BodyPublishers.ofString(body))
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

                // The body the servlet echoed back should match
                // EXACTLY what we sent — proves the filter didn't
                // corrupt the wrapped stream during pre-read.
                assertEquals(200, resp.statusCode());
                assertEquals(body, resp.body(),
                        "echo body must match the request body — wrapper's replay "
                                + "must deliver the exact bytes the client sent");
            } finally {
                server.stop();
            }
        }

        // Capture line emitted exactly once. The logger drains on
        // close (above), so by here sink.lines() reflects every
        // event that made it through.
        assertEquals(1, sink.lines().size(),
                "exactly one capture line expected; got " + sink.lines());
        String line = sink.lines().get(0);
        assertTrue(line.contains("\"method\":\"POST\""),
                "method captured: " + line);
        assertTrue(line.contains("/echo?trace=abc"),
                "URL with query string captured: " + line);
        assertTrue(line.contains("\"Content-Type\":\"application/json\""),
                "request Content-Type captured: " + line);
        assertTrue(line.contains("\"X-Trace-Id\":\"req-42\""),
                "custom header captured: " + line);
        assertTrue(line.contains("\\\"id\\\":\\\"x-1\\\""),
                "body field captured and JSON-escaped: " + line);
        assertFalse(line.contains("\"status\""),
                "filter is a REQUEST capture — no response status field expected");
    }
}
