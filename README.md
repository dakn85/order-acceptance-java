# Gauge Java · Securities Order Acceptance

Das Projekt bildet denselben fachlichen Order-Demo-Vertrag wie das
`securities-order-acceptance`-Projekt mit `gauge-ts` ab. Es zeigt bewusst drei
getrennte Testebenen:

- UI mit Playwright Java gegen den Order Desk,
- REST mit OkHttp gegen den Order Command Service,
- Kafka mit dem Java-Consumer gegen das Confluent-Avro-Event.

`default` ist die Plattformumgebung und verwendet ausdrücklich die internen
Compose-/Kubernetes-Adressen `order-ui:8083`, `order-service:8080` und
`redpanda:9092`:

```sh
gauge run --env default specs
```

`mock` ist ausschließlich der deterministische Entwicklungs-/Pipeline-Modus.
Er verwendet eine lokale HTML-Seite, `MockWebServer` und `MockProducer`, aber
exakt dieselben Gauge-Bindings und benötigt keine Demo-Dienste:

```sh
gauge run --env mock specs
```

Der KnippQAi-Runner verwendet Java 25 und kompiliert dieses Projekt mit
`--release 21`. Maven-Abhängigkeiten, Gauge-Java-Classpath, Playwright-Browser,
Evidence und Laufzeitmetriken werden durch den Runner bzw. den injizierten
Java-Agenten bereitgestellt. Das Testprojekt benötigt keine KnippQAi-Hooks und
keine Änderungen für die Plattformintegration.
