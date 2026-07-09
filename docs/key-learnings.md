# Key Learnings — Quiz

Test your understanding of the MicroProfile Reactive Messaging guide.

---

**Q1. What are the two microservices in this guide and what does each one do?**

<details>
<summary>Answer</summary>

| Service                    | Port | Responsibility                                                                                               |
| -------------------------- | ---- | ------------------------------------------------------------------------------------------------------------ |
| **System Microservice**    | 9083 | Reads its own hostname and CPU load average every 15 seconds and publishes them to Kafka                     |
| **Inventory Microservice** | 9085 | Listens to Kafka for system load events, stores them in memory, and exposes REST endpoints to query the data |

</details>

---

**Q2. What annotation does the System Microservice use to publish messages, and what does the method signature look like?**

<details>
<summary>Answer</summary>

It uses `@Outgoing("systemLoad")` on a method that returns a reactive `Publisher<SystemLoad>`:

```java
@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(15, TimeUnit.SECONDS)
            .map((interval -> new SystemLoad(getHostname(),
                    Double.valueOf(OS_MEAN.getSystemLoadAverage()))));
}
```

The MicroProfile Reactive Messaging runtime subscribes to the returned `Publisher` and forwards each emitted item to the configured Kafka connector. The method only needs to declare _what_ to produce — the runtime handles the rest.

</details>

---

**Q3. What annotation does the Inventory Microservice use to consume messages, and what does the method signature look like?**

<details>
<summary>Answer</summary>

It uses `@Incoming("systemLoad")` on a void method that accepts a deserialized `SystemLoad` object:

```java
@Incoming("systemLoad")
public void updateStatus(SystemLoad sl) {
    String hostname = sl.hostname;
    if (manager.getSystem(hostname).isPresent()) {
        manager.updateCpuStatus(hostname, sl.loadAverage);
    } else {
        manager.addSystem(hostname, sl.loadAverage);
    }
}
```

The runtime calls this method for each incoming message — there is no manual Kafka polling, offset tracking, or consumer group management in application code.

</details>

---

**Q4. What is the relationship between the channel name in `@Outgoing`/`@Incoming` and the Kafka topic name?**

<details>
<summary>Answer</summary>

They are separate. The annotation value (e.g., `"systemLoad"`) is a **logical channel name** used inside the application. The physical **Kafka topic name** (`system.load`) is declared in `microprofile-config.properties`:

```properties
# System Microservice — producer
mp.messaging.outgoing.systemLoad.topic=system.load

# Inventory Microservice — consumer
mp.messaging.incoming.systemLoad.topic=system.load
```

This separation lets the same code be pointed at a different topic per environment without changing or recompiling the Java source.

</details>

---

**Q5. What is the `group.id` consumer property on the Inventory Microservice and why does it matter?**

<details>
<summary>Answer</summary>

```properties
mp.messaging.incoming.systemLoad.group.id=system-load-status
```

`group.id` identifies the **Kafka consumer group** that this service instance belongs to. When multiple Inventory Microservice instances share the same `group.id`, Kafka distributes partitions across them — each message is delivered to exactly one instance. If the `group.id` were omitted or unique per instance, every instance would receive every message independently, causing duplicate processing.

</details>

---

**Q6. How are `SystemLoad` objects converted to and from bytes on the Kafka wire?**

<details>
<summary>Answer</summary>

`SystemLoad` defines two static inner classes backed by Jakarta JSON Binding (JSONB):

```java
public static class SystemLoadSerializer implements Serializer<Object> {
    @Override
    public byte[] serialize(String topic, Object data) {
        return JSONB.toJson(data).getBytes();
    }
}

public static class SystemLoadDeserializer implements Deserializer<SystemLoad> {
    @Override
    public SystemLoad deserialize(String topic, byte[] data) {
        if (data == null) return null;
        return JSONB.fromJson(new String(data), SystemLoad.class);
    }
}
```

They are wired in config:

```properties
# Producer side (System Microservice)
mp.messaging.outgoing.systemLoad.value.serializer=io.openliberty.guides.models.SystemLoad$SystemLoadSerializer

# Consumer side (Inventory Microservice)
mp.messaging.incoming.systemLoad.value.deserializer=io.openliberty.guides.models.SystemLoad$SystemLoadDeserializer
```

Placing both classes inside `SystemLoad` co-locates serialization logic with the model it owns, and both services reference the same inner class paths in config — guaranteeing wire format consistency.

</details>

---

**Q7. Why does the System Microservice use `Flowable.interval` from RxJava3 rather than a simple scheduled loop?**

<details>
<summary>Answer</summary>

`Flowable.interval` produces a **back-pressure-aware reactive stream**. The MicroProfile Reactive Messaging runtime subscribes to it and controls the emission rate — if the downstream Kafka connector is slow, the stream respects that pressure rather than unboundedly buffering items.

A scheduled loop (`ScheduledExecutorService`) would push messages regardless of consumer capacity and would require the application to manage thread lifecycle manually. With `Flowable.interval`, the runtime handles threading and scheduling, and the application code stays declarative:

```java
return Flowable.interval(15, TimeUnit.SECONDS)
        .map(interval -> new SystemLoad(getHostname(), loadAvg));
```

</details>

---

**Q8. What data does the `SystemLoad` model carry and how is it structured?**

<details>
<summary>Answer</summary>

```java
public class SystemLoad {
    public String hostname;   // e.g. "inventory-service-host"
    public Double loadAverage; // system CPU load; -1.0 if unavailable
}
```

The two fields are public — JSONB serializes them directly by field name. The `OperatingSystemMXBean.getSystemLoadAverage()` returns `-1.0` on systems that do not support the measurement (e.g., some Windows environments).

</details>

---

**Q9. How does the Inventory Microservice store incoming system load data, and why is thread safety needed?**

<details>
<summary>Answer</summary>

`InventoryManager` uses a `synchronized TreeMap`:

```java
private Map<String, Properties> systems = Collections.synchronizedMap(
    new TreeMap<String, Properties>());
```

Thread safety is needed because `@Incoming` messages can be delivered on different threads by the reactive messaging runtime, while REST endpoint handlers (`getSystems()`, `getSystem()`) may read the map concurrently on Jakarta EE managed threads. Without synchronization, concurrent reads and writes to `TreeMap` would produce unpredictable results.

`TreeMap` is chosen over `HashMap` because it keeps hostnames in sorted order, making `GET /inventory/systems` responses deterministic.

</details>

---

**Q10. What is the bootstrap servers property and where must it be set?**

<details>
<summary>Answer</summary>

`mp.messaging.connector.liberty-kafka.bootstrap.servers` is the entry point address for the Kafka cluster. It must be set in each service that uses the `liberty-kafka` connector:

```properties
# Same property in both system and inventory microprofile-config.properties
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092
```

`kafka:9092` is the Docker Compose network alias and internal port of the Kafka container. For integration tests, Testcontainers overrides this via an environment variable passed to the inventory container:

```java
inventoryContainer.withEnv(
    "mp.messaging.connector.liberty-kafka.bootstrap.servers",
    "kafka:19092");
```

This is a key example of how MicroProfile Config lets environment-specific values be injected without code changes.

</details>

---

**Q11. How does the integration test verify that the Inventory Microservice correctly processes Kafka messages?**

<details>
<summary>Answer</summary>

The test publishes a `SystemLoad` message directly via a `KafkaProducer` (bypassing the System Microservice container), waits 5 seconds for the Inventory Microservice to consume and store it, then queries the REST API and asserts the stored values match:

```java
SystemLoad sl = new SystemLoad("localhost", 1.1);
producer.send(new ProducerRecord<>("system.load", sl));
Thread.sleep(5000);

Response response = client.getSystems();
List<Properties> systems = response.readEntity(new GenericType<>() {});
assertEquals(200, response.getStatus());
assertEquals(1, systems.size());
BigDecimal systemLoad = (BigDecimal) systems.get(0).get("systemLoad");
assertEquals(sl.loadAverage, systemLoad.doubleValue());
```

By injecting a controlled payload and asserting via REST, the test exercises the full path: Kafka → `@Incoming` listener → `InventoryManager` → `GET /inventory/systems` — without depending on the System Microservice being present.

</details>
