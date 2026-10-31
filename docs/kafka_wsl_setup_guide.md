# Connecting a Windows Java Project to Kafka on WSL

This guide outlines the necessary configurations to allow a Java application running on Windows to successfully send and receive messages from a Kafka broker running inside Windows Subsystem for Linux (WSL).

---

## 1. Configure Kafka in WSL (`server.properties`)

Open your Kafka configuration file inside WSL:
```bash
nano config/server.properties
```

Update or add the following properties (replace `172.26.152.7` with your specific WSL IP address obtained via `hostname -I`):

```properties
# Listen on all network interfaces inside WSL
listeners=PLAINTEXT://0.0.0.0:9092

# Tell clients how to reach Kafka (exclude any subnet masks like /20)
advertised.listeners=PLAINTEXT://172.26.152.7:9092
```

> **Note:** Remember to restart Zookeeper and your Kafka broker after modifying this file.

---

## 2. Configure Windows Firewall

To prevent Windows from blocking incoming TCP traffic on Kafka's port from the WSL bridge, open **PowerShell as Administrator** and run:

```powershell
New-NetFirewallRule -DisplayName "Kafka WSL Port 9092" -Direction Inbound -Protocol TCP -LocalPort 9092 -Action Allow
```

---

## 3. Configure Your Java Application

In your Java project running on Windows, configure your Kafka `ProducerConfig` (or your `application.yml` / `application.properties`) to point to the WSL IP address:

### Java Code Example:
```java
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

Properties properties = new Properties();
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.26.152.7:9092");
// Add other producer configurations (e.g., serializers) here...
```

### Spring Boot (`application.yml`) Example:
```yaml
spring:
  kafka:
    bootstrap-servers: 172.26.152.7:9092
```