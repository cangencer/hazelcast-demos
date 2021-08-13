import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.javalin.Javalin;
import io.javalin.websocket.WsContext;
import org.json.JSONObject;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;

import static com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN;
import static com.hazelcast.client.properties.ClientProperty.STATISTICS_ENABLED;

public class WebServer {

    private static final Map<String, WsContext> sessions = new ConcurrentHashMap<>();
    private static final Map<String, List<WsContext>> symbolsToBeUpdated = new ConcurrentHashMap<>();
    private static final HazelcastInstance client = getHzInstance();

    public static void main(String[] args) {
        IMap<String, Tuple3<Long, Long, Integer>> results = client.getMap("query1_Results");
        IMap<String, HazelcastJsonValue> trades = client.getMap("trades");
        IMap<String, String> symbols = client.getMap("symbols");

        trades.addEntryListener(new TradeRecordsListener(), true);

        Javalin app = Javalin.create().start(9000);
        app.config
                .addStaticFiles("/app")                                     // The ReactJS application
                .addSinglePageRoot("/", "/app/index.html");   // Catch-all route for the single-page application

        app.ws("/trades", wsHandler -> {
            wsHandler.onConnect(ctx -> {
                String sessionId = ctx.getSessionId();
                System.out.println("Starting the session -> " + sessionId);
                sessions.put(sessionId, ctx);
            });

            wsHandler.onClose(ctx -> {
                String sessionId = ctx.getSessionId();
                System.out.println("Closing the session -> " + sessionId);
                sessions.remove(sessionId, ctx);
                for (Entry<String, List<WsContext>> entry : symbolsToBeUpdated.entrySet()) {
                    List<WsContext> contexts = entry.getValue();
                    contexts.removeIf(context -> context.getSessionId().equals(sessionId));
                }
            });

            wsHandler.onMessage(ctx -> {
                String sessionId = ctx.getSessionId();
                String message = ctx.message();
                WsContext session = sessions.get(sessionId);

                if ("LOAD_SYMBOLS".equals(message)) {
                    JSONObject jsonObject = new JSONObject();
                    Map<String, String> allSymbols = symbols.entrySet().stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
                    results.forEach((key, value) -> {
                        jsonObject.append("symbols", new JSONObject()
                                .put("name", allSymbols.get(key))
                                .put("symbol", key)
                                .put("count", value.f0())
                                .put("volume", priceToString(value.f1()))
                                .put("price", value.f2())
                        );
                    });
                    session.send(jsonObject.toString());
                } else if (message.startsWith("DRILL_SYMBOL")) {
                    String symbol = message.split(" ")[1];
                    System.out.println("Session -> " + sessionId + " requested symbol -> " + symbol);
                    symbolsToBeUpdated.compute(symbol, (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(session);
                        return v;
                    });

                    JSONObject jsonObject = new JSONObject();
                    client.getSql().execute("select id, \"timestamp\", symbol, price, quantity from trades "
                            + "where symbol = ?", symbol)
                        .forEach(record -> {
                            JSONObject tradeObject = new JSONObject();
                            tradeObject.put("id", record.<String>getObject("id"));
                            tradeObject.put("timestamp", record.<Long>getObject("timestamp"));
                            tradeObject.put("symbol", record.<String>getObject("symbol"));
                            tradeObject.put("price", record.<Integer>getObject("price"));
                            tradeObject.put("quantity", record.<Integer>getObject("quantity"));
                            jsonObject.put("symbol", symbol);
                            jsonObject.append("data", tradeObject);
                            }
                        );
                    session.send(jsonObject.toString());
                }
            });
        });
    }

    private static class TradeRecordsListener implements EntryAddedListener<String, HazelcastJsonValue> {

        @Override
        public void entryAdded(EntryEvent<String, HazelcastJsonValue> event) {
            JSONObject tradeJson = new JSONObject(event.getValue().toString());
            String symbol = tradeJson.getString("symbol");
            List<WsContext> contexts = symbolsToBeUpdated.get(symbol);
            if (contexts != null && !contexts.isEmpty()) {
                System.out.println("Broadcasting update on = " + symbol);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("symbol", symbol);
                jsonObject.append("data", tradeJson);
                String message = jsonObject.toString();
                for (WsContext context : contexts) {
                    context.send(message);
                }
            }
        }
    }

    private static String priceToString(long price) {
        return String.format("$%,.2f", price / 100.0d);
    }

    private static HazelcastInstance getHzInstance() {
        ClientConfig config = new ClientConfig();
        config.setProperty(STATISTICS_ENABLED.getName(), "true");
        String discoveryToken = System.getenv("DISCOVERY_TOKEN");
        Objects.requireNonNull(discoveryToken, "DISCOVERY_TOKEN is not provided");
        String cloudUrl = System.getenv("CLOUD_URL");
        Objects.requireNonNull(cloudUrl, "CLOUD_URL is not provided");
        String clusterName = System.getenv("CLUSTER_NAME");
        Objects.requireNonNull(clusterName, "CLUSTER_NAME is not provided");
        config.setProperty(HAZELCAST_CLOUD_DISCOVERY_TOKEN.getName(), discoveryToken);
        config.setProperty("hazelcast.client.cloud.url", cloudUrl);
        config.setClusterName(clusterName);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        System.out.println("Connection Successful!");
        return client;
    }

}
