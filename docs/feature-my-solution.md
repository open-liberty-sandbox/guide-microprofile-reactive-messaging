# feature/my-solution — Change Log

This document summarises the key additions that implement MicroProfile Reactive Messaging between the System Microservice and Inventory Microservice via a Kafka message broker.

---

## 1. Publish system load metrics to Kafka (`SystemService.java`)

**Commit:** `feat: emit SystemLoad events on outgoing systemLoad channel`

### What changed

The `sendSystemLoad()` method was added to `SystemService` with the `@Outgoing("systemLoad")` annotation, returning a reactive `Publisher<SystemLoad>`:

```java
@Outgoing("systemLoad")
public Publisher<SystemLoad> sendSystemLoad() {
    return Flowable.interval(15, TimeUnit.SECONDS)
            .map((interval -> new SystemLoad(getHostname(),
                    Double.valueOf(OS_MEAN.getSystemLoadAverage()))));
}
```

A helper `getHostname()` was also added to resolve the container's hostname via `InetAddress.getLocalHost()`, falling back to the `HOSTNAME` environment variable:

```java
private static String getHostname() {
    if (hostname == null) {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return System.getenv("HOSTNAME");
        }
    }
    return hostname;
}
```

### Why

`@Outgoing("systemLoad")` declares that this method produces messages onto a named channel. The MicroProfile Reactive Messaging runtime subscribes to the returned `Publisher` and forwards each emitted item to the configured connector (Kafka). Using RxJava3's `Flowable.interval` produces a periodic, back-pressure-aware stream — the runtime is never pushed more items than it can handle, and the emission schedule is guaranteed to be non-blocking for the application thread.

---

## 2. Consume system load events from Kafka (`InventoryResource.java`)

**Commit:** `feat: add updateStatus to consume incoming systemLoad messages`

### What changed

The `updateStatus()` method was added to `InventoryResource` with the `@Incoming("systemLoad")` annotation:

```java
@Incoming("systemLoad")
public void updateStatus(SystemLoad sl) {
    String hostname = sl.hostname;
    if (manager.getSystem(hostname).isPresent()) {
        manager.updateCpuStatus(hostname, sl.loadAverage);
        logger.info("Host " + hostname + " was updated: " + sl);
    } else {
        manager.addSystem(hostname, sl.loadAverage);
        logger.info("Host " + hostname + " was added: " + sl);
    }
}
```

### Why

`@Incoming("systemLoad")` tells the runtime to call this method for every message arriving on the `systemLoad` channel. The method is a simple void consumer — no manual Kafka polling, offset management, or thread handling is needed. The runtime delivers deserialized `SystemLoad` objects directly, so the business logic (add vs update in `InventoryManager`) is kept clean and decoupled from messaging infrastructure.

---

## 3. Wire channels to Kafka via MicroProfile Config

**Commit:** `feat: add microprofile-config.properties for system and inventory Kafka binding`

### What changed — System Microservice

`system/src/main/resources/META-INF/microprofile-config.properties` binds the `systemLoad` outgoing channel to the `system.load` Kafka topic with the custom JSONB serializer:

```properties
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092

mp.messaging.outgoing.systemLoad.connector=liberty-kafka
mp.messaging.outgoing.systemLoad.topic=system.load
mp.messaging.outgoing.systemLoad.key.serializer=org.apache.kafka.common.serialization.StringSerializer
mp.messaging.outgoing.systemLoad.value.serializer=io.openliberty.guides.models.SystemLoad$SystemLoadSerializer
```

### What changed — Inventory Microservice

`inventory/src/main/resources/META-INF/microprofile-config.properties` binds the `systemLoad` incoming channel to the same `system.load` topic with the custom JSONB deserializer and a consumer group ID:

```properties
mp.messaging.connector.liberty-kafka.bootstrap.servers=kafka:9092

mp.messaging.incoming.systemLoad.connector=liberty-kafka
mp.messaging.incoming.systemLoad.topic=system.load
mp.messaging.incoming.systemLoad.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
mp.messaging.incoming.systemLoad.value.deserializer=io.openliberty.guides.models.SystemLoad$SystemLoadDeserializer
mp.messaging.incoming.systemLoad.group.id=system-load-status
```

### Why

The channel names in `@Outgoing`/`@Incoming` are logical names — they must be mapped to a physical transport at runtime. MicroProfile Config is the glue: the property key format `mp.messaging.[outgoing|incoming].<channel-name>.<property>` is the standard contract. Keeping the Kafka topic name (`system.load`) and broker address out of Java code means both can be changed per-environment (dev, staging, prod) without recompilation.

The `group.id` on the consumer side (`system-load-status`) ensures that if multiple Inventory Microservice instances are running, Kafka distributes messages across them rather than delivering every message to every instance.

---

## 4. Add custom Kafka serializer and deserializer (`SystemLoad.java`)

**Commit:** `feat: add JSONB-backed Kafka serializer and deserializer to SystemLoad model`

### What changed

`SystemLoad` was given two static inner classes implementing Kafka's `Serializer` and `Deserializer` interfaces, backed by Jakarta JSON Binding (JSONB):

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
        if (data == null) {
            return null;
        }
        return JSONB.fromJson(new String(data), SystemLoad.class);
    }
}
```

A shared static `Jsonb` instance is used for both:

```java
private static final Jsonb JSONB = JsonbBuilder.create();
```

### Why

Kafka transmits raw bytes — the application is responsible for defining how objects are converted to and from bytes. Placing the serializer and deserializer as inner classes of `SystemLoad` keeps the serialization logic co-located with the data model it owns, and both ends (System Microservice producer, Inventory Microservice consumer) reference the same inner class paths in their config, guaranteeing wire format consistency.

---

## 5. Create the integration test (`InventoryServiceIT.java`)

**Commit:** `feat: create InventoryServiceIT integration test`

### What changed

`InventoryServiceIT.java` was created to verify that the Inventory Microservice correctly receives Kafka messages and stores them so REST queries return accurate data. The test uses Testcontainers to spin up a real `KafkaContainer` and the inventory application container:

```java
@Test
public void testCpuUsage() throws InterruptedException {
    SystemLoad sl = new SystemLoad("localhost", 1.1);
    producer.send(new ProducerRecord<String, SystemLoad>("system.load", sl));
    Thread.sleep(5000);
    Response response = client.getSystems();
    List<Properties> systems =
        response.readEntity(new GenericType<List<Properties>>() { });
    assertEquals(200, response.getStatus(), "Response should be 200");
    Assertions.assertEquals(systems.size(), 1);
    for (Properties system : systems) {
        assertEquals(sl.hostname, system.get("hostname"), "Hostname doesn't match!");
        BigDecimal systemLoad = (BigDecimal) system.get("systemLoad");
        assertEquals(sl.loadAverage, systemLoad.doubleValue(), "CPU load doesn't match!");
    }
}
```

A real `KafkaProducer` with `SystemLoadSerializer` publishes the message directly, bypassing the System Microservice container:

```java
producerProps.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    SystemLoadSerializer.class.getName());
producer = new KafkaProducer<String, SystemLoad>(producerProps);
```

The test also supports running against a live `liberty:devc` instance by checking if port 9085 is already open before starting containers.

### Why

The test injects a controlled `SystemLoad` message (`localhost`, load `1.1`) directly into the `system.load` Kafka topic, then queries the Inventory Microservice via REST to assert that the message was received, deserialized correctly, and stored. This end-to-end path exercises the `@Incoming("systemLoad")` listener, `SystemLoadDeserializer`, `InventoryManager` storage, and the `GET /inventory/systems` REST endpoint together — something a unit test mocking the Kafka consumer could not cover.
