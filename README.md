# BikeStore Async v1.0

Pedidos asincronos con RabbitMQ. Flujo:
`OrderProducer` -> `cola.pagos` -> `PaymentWorker` -> `cola.correos` -> `EmailWorker`,
con reintentos por cola de espera (TTL) y Dead-Letter Queue.

## Requisitos
JDK 17+, Maven 3.8+ y Docker (solo para el broker).

## Como ejecutarlo
En la carpeta del proyecto, una terminal por cada paso:

```bash
docker compose up -d      # broker; consola web en http://localhost:15672 (guest / guest)

mvn -q compile exec:java -Dexec.mainClass=tienda.PaymentWorker
mvn -q compile exec:java -Dexec.mainClass=tienda.EmailWorker

# productor, opcion A: API HTTP
mvn -q compile exec:java -Dexec.mainClass=tienda.OrderProducer
# productor, opcion B: 30 pedidos de prueba (para ver reintentos y DLQ)
mvn -q compile exec:java -Dexec.mainClass=tienda.OrderProducer -Dexec.args="demo 30"
```

Llamar a la API (bash):

```bash
curl -X POST http://localhost:8080/api/ordenar -H "Content-Type: application/json" \
  -d '{"cliente":"Ariel","correo":"ariel@correo.com","producto":"Bicicleta MTB","cantidad":1,"total":450.0}'
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/ordenar -ContentType "application/json" `
  -Body '{"cliente":"Ariel","correo":"ariel@correo.com","producto":"Bicicleta MTB","cantidad":1,"total":450.0}'
```

## Topologia
| Elemento | Nombre | Para que sirve |
|---|---|---|
| Exchange | tienda.pedidos | pedidos nuevos y resultados de pago |
| Cola | cola.pagos | la consume PaymentWorker |
| Exchange + cola de espera | tienda.espera / cola.pagos.espera | retiene 3 s el pedido fallido y lo devuelve a cola.pagos al vencer el TTL |
| Cola | cola.correos | la consume EmailWorker |
| Exchange + DLQ | tienda.muertos / cola.pagos.dlq | pedidos que fallaron los 3 reintentos (y mensajes ilegibles) |

El numero de reintentos viaja en el header `reintentos` del mensaje. Se permiten 1 intento inicial + 3 reintentos; con 50 % de fallo, cerca de 6 % de los pedidos llega a la DLQ.
Para ver la DLQ: consola web -> Queues -> `cola.pagos.dlq` -> Get messages.
