package tienda;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

/** Consumidor 2. Solo envia el correo de confirmacion cuando paymentStatus = PAID. */
public class EmailWorker {

    public static void main(String[] args) throws Exception {
        Connection conexion = Broker.conectar();
        Channel canal = conexion.createChannel();
        Broker.declarar(canal);
        canal.basicQos(1);
        Bitacora.registrar("EmailWorker", "-", "esperando resultados en " + Broker.COLA_CORREOS);

        DeliverCallback alRecibir = (etiqueta, entrega) -> {
            long tag = entrega.getEnvelope().getDeliveryTag();
            ResultadoPago resultado = Broker.JSON.readValue(entrega.getBody(), ResultadoPago.class);
            Bitacora.registrar("EmailWorker", resultado.pedidoId(),
                    "resultado de pago recibido: " + resultado.paymentStatus());

            if ("PAID".equals(resultado.paymentStatus())) {
                Bitacora.pausa(200); // simula el envio SMTP
                Bitacora.registrar("EmailWorker", resultado.pedidoId(),
                        "correo de confirmacion enviado a " + resultado.correo());
            } else {
                Bitacora.registrar("EmailWorker", resultado.pedidoId(),
                        "pago no confirmado, no se envia correo");
            }
            canal.basicAck(tag, false);
        };

        canal.basicConsume(Broker.COLA_CORREOS, false, alRecibir, etiqueta -> {});
    }
}
