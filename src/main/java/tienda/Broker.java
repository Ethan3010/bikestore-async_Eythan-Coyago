package tienda;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuracion del broker y declaracion de la topologia.
 *
 * Los reintentos se hacen en el propio broker con una cola de espera:
 *   cola.pagos --(fallo)--> tienda.espera --> cola.pagos.espera (TTL 3 s)
 *        ^                                            |
 *        |____ tienda.pedidos (dead-letter al expirar)|
 * y cuando se agotan los reintentos el mensaje se publica en tienda.muertos -> cola.pagos.dlq
 */
public final class Broker {

    private Broker() {}

    public static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static final String EX_PEDIDOS = "tienda.pedidos";
    public static final String EX_ESPERA = "tienda.espera";
    public static final String EX_MUERTOS = "tienda.muertos";

    public static final String COLA_PAGOS = "cola.pagos";
    public static final String COLA_ESPERA = "cola.pagos.espera";
    public static final String COLA_DLQ = "cola.pagos.dlq";
    public static final String COLA_CORREOS = "cola.correos";

    public static final String RK_PEDIDO_CREADO = "pedido.creado";
    public static final String RK_PAGO_RESULTADO = "pago.resultado";
    public static final String RK_ESPERA = "pago.espera";
    public static final String RK_MUERTO = "pago.muerto";

    public static final int MAX_REINTENTOS = 3;
    public static final int ESPERA_MS = 3000;
    public static final String HEADER_REINTENTOS = "reintentos";

    public static Connection conectar() throws Exception {
        ConnectionFactory fabrica = new ConnectionFactory();
        fabrica.setHost(System.getenv().getOrDefault("RABBIT_HOST", "localhost"));
        fabrica.setPort(5672);
        fabrica.setUsername("guest");
        fabrica.setPassword("guest");
        return fabrica.newConnection();
    }

    /** Es idempotente: cada aplicacion la ejecuta al arrancar. */
    public static void declarar(Channel canal) throws Exception {
        canal.exchangeDeclare(EX_PEDIDOS, BuiltinExchangeType.DIRECT, true);
        canal.exchangeDeclare(EX_ESPERA, BuiltinExchangeType.DIRECT, true);
        canal.exchangeDeclare(EX_MUERTOS, BuiltinExchangeType.DIRECT, true);

        // Dead-Letter Queue (analisis offline)
        canal.queueDeclare(COLA_DLQ, true, false, false, null);
        canal.queueBind(COLA_DLQ, EX_MUERTOS, RK_MUERTO);

        // Cola principal de pagos; un mensaje ilegible (nack sin requeue) tambien termina en la DLQ
        Map<String, Object> argsPagos = new HashMap<>();
        argsPagos.put("x-dead-letter-exchange", EX_MUERTOS);
        argsPagos.put("x-dead-letter-routing-key", RK_MUERTO);
        canal.queueDeclare(COLA_PAGOS, true, false, false, argsPagos);
        canal.queueBind(COLA_PAGOS, EX_PEDIDOS, RK_PEDIDO_CREADO);

        // Cola de espera: nadie la consume, al vencer el TTL el mensaje vuelve a la cola de pagos
        Map<String, Object> argsEspera = new HashMap<>();
        argsEspera.put("x-message-ttl", ESPERA_MS);
        argsEspera.put("x-dead-letter-exchange", EX_PEDIDOS);
        argsEspera.put("x-dead-letter-routing-key", RK_PEDIDO_CREADO);
        canal.queueDeclare(COLA_ESPERA, true, false, false, argsEspera);
        canal.queueBind(COLA_ESPERA, EX_ESPERA, RK_ESPERA);

        // Cola de correos
        canal.queueDeclare(COLA_CORREOS, true, false, false, null);
        canal.queueBind(COLA_CORREOS, EX_PEDIDOS, RK_PAGO_RESULTADO);
    }

    public static AMQP.BasicProperties propiedades(int reintentos) {
        Map<String, Object> headers = new HashMap<>();
        headers.put(HEADER_REINTENTOS, reintentos);
        return new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2) // persistente
                .headers(headers)
                .build();
    }
}
