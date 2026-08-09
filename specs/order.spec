# Java acceptance tests against the securities order demo

Tags: java, order-demo

## UI — trader submits an order through the real order desk

* The trader opens the order entry page
* The trader submits a "BUY" order for "10" "SAP" at "180.00" EUR through the UI
* The order blotter shows "SAP" with status "PARTIALLY_FILLED"

## REST — command API accepts a valid limit order

* Customer "alice" submits a limit "BUY" order for "10" "SAP" at "180.00" EUR through REST
* The order command API accepts the order with status "NEW"

## Kafka — accepted command emits a Confluent Avro event

* Customer "pension-core" submits a limit "SELL" order for "4" "SIE" at "176.30" EUR through REST
* An order-created event exists for the order on topic "orders.created"
