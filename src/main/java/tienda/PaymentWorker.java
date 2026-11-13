package tienda;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Consumidor 1. Simula el cobro con tarjeta (50 % de fallo).
 *  - Cobro correcto: publica PAID y confirma el mensaje.
 *  - Cobro fallido con reintentos disponibles: lo manda a la cola de espera (3 s) con el contador + 1.
 *  - Cobro fallido tras 3 reintentos: lo publica en la Dead-Letter Queue y publica FAILED.
 */
public class PaymentWorker {

    private static final double PROBABILIDAD_FALLO = 0.5;

    public static void main(String[] args) throws Exception {
        Connection conexion = Broker.conectar();
        Channel canal = conexion.createChannel();
        Broker.declarar(canal);
        canal.basicQos(1);
        Bitacora.registrar("PaymentWorker", "-", "esperando pedidos en " + Broker.COLA_PAGOS);

        DeliverCallback alRecibir = (etiqueta, entrega) -> {
            long tag = entrega.getEnvelope().getDeliveryTag();

            Pedido pedido;
            try {
                pedido = Broker.JSON.readValue(entrega.getBody(), Pedido.class);
            } catch (IOException e) {
                Bitacora.registrar("PaymentWorker", "?", "mensaje ilegible, va a la DLQ");
                canal.basicNack(tag, false, false); // el dead-letter de la cola lo desvia
                return;
            }

            int reintentosHechos = leerReintentos(entrega.getProperties());
            int intento = reintentosHechos + 1;
            Bitacora.registrar("PaymentWorker", pedido.pedidoId(),
                    "pedido recibido, intento " + intento + " de " + (Broker.MAX_REINTENTOS + 1));

            if (cobrar(pedido)) {
                publicarResultado(canal, pedido, "PAID", intento);
                canal.basicAck(tag, false);
            } else if (reintentosHechos < Broker.MAX_REINTENTOS) {
                Bitacora.registrar("PaymentWorker", pedido.pedidoId(),
                        "reintento " + (reintentosHechos + 1) + " de " + Broker.MAX_REINTENTOS
                                + " programado en " + Broker.ESPERA_MS + " ms");
                canal.basicPublish(Broker.EX_ESPERA, Broker.RK_ESPERA,
                        Broker.propiedades(reintentosHechos + 1), entrega.getBody());
                canal.basicAck(tag, false);
            } else {
                Bitacora.registrar("PaymentWorker", pedido.pedidoId(),
                        "reintentos agotados, mensaje enviado a " + Broker.COLA_DLQ);
                canal.basicPublish(Broker.EX_MUERTOS, Broker.RK_MUERTO,
                        Broker.propiedades(reintentosHechos), entrega.getBody());
                publicarResultado(canal, pedido, "FAILED", intento);
                canal.basicAck(tag, false);
            }
        };

        canal.basicConsume(Broker.COLA_PAGOS, false, alRecibir, etiqueta -> {});
    }

    private static boolean cobrar(Pedido pedido) {
        Bitacora.pausa(300); // latencia simulada de la pasarela
        boolean aprobado = ThreadLocalRandom.current().nextDouble() >= PROBABILIDAD_FALLO;
        Bitacora.registrar("PaymentWorker", pedido.pedidoId(),
                aprobado ? "cobro aprobado" : "cobro rechazado (fallo simulado)");
        return aprobado;
    }

    private static void publicarResultado(Channel canal, Pedido p, String estado, int intentos) throws IOException {
        ResultadoPago resultado = new ResultadoPago(p.pedidoId(), p.cliente(), p.correo(), estado, intentos);
        canal.basicPublish(Broker.EX_PEDIDOS, Broker.RK_PAGO_RESULTADO,
                Broker.propiedades(0), Broker.JSON.writeValueAsBytes(resultado));
        Bitacora.registrar("PaymentWorker", p.pedidoId(),
                "resultado " + estado + " publicado tras " + intentos + " intento(s)");
    }

    private static int leerReintentos(AMQP.BasicProperties propiedades) {
        Map<String, Object> headers = propiedades.getHeaders();
        if (headers == null) return 0;
        Object valor = headers.get(Broker.HEADER_REINTENTOS);
        return valor instanceof Number ? ((Number) valor).intValue() : 0;
    }
}
