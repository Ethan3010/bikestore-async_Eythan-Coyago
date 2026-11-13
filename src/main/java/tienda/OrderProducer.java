package tienda;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * API "Ordenar". Convierte la peticion en un mensaje JSON y lo publica en el broker.
 *
 *   java ... tienda.OrderProducer          -> servidor en POST http://localhost:8080/api/ordenar
 *   java ... tienda.OrderProducer demo 10  -> publica 10 pedidos de prueba y termina
 */
public class OrderProducer {

    record SolicitudOrden(String cliente, String correo, String producto, int cantidad, double total) {}

    private static Channel canal;

    public static void main(String[] args) throws Exception {
        Connection conexion = Broker.conectar();
        canal = conexion.createChannel();
        Broker.declarar(canal);

        if (args.length == 2 && args[0].equals("demo")) {
            int cantidad = Integer.parseInt(args[1]);
            for (int i = 1; i <= cantidad; i++) {
                publicar(new SolicitudOrden("Cliente " + i, "cliente" + i + "@correo.com",
                        "Bicicleta modelo " + i, 1, 300.0 + i * 10));
            }
            canal.close();
            conexion.close();
            return;
        }

        HttpServer servidor = HttpServer.create(new InetSocketAddress(8080), 0);
        servidor.setExecutor(Executors.newFixedThreadPool(4));
        servidor.createContext("/api/ordenar", OrderProducer::atender);
        servidor.start();
        Bitacora.registrar("OrderProducer", "-", "API disponible en POST http://localhost:8080/api/ordenar");
    }

    private static void atender(HttpExchange intercambio) throws IOException {
        try {
            if (!"POST".equals(intercambio.getRequestMethod())) {
                responder(intercambio, 405, "{\"error\":\"solo se acepta POST\"}");
                return;
            }
            byte[] cuerpo = intercambio.getRequestBody().readAllBytes();
            SolicitudOrden solicitud = Broker.JSON.readValue(cuerpo, SolicitudOrden.class);
            if (solicitud.cliente() == null || solicitud.correo() == null) {
                responder(intercambio, 400, "{\"error\":\"faltan cliente y/o correo\"}");
                return;
            }
            String id = publicar(solicitud);
            responder(intercambio, 202, "{\"pedidoId\":\"" + id + "\",\"estado\":\"EN_COLA\"}");
        } catch (Exception e) {
            responder(intercambio, 400, "{\"error\":\"peticion invalida\"}");
        }
    }

    private static String publicar(SolicitudOrden s) throws IOException {
        String id = "PED-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        Pedido pedido = new Pedido(id, s.cliente(), s.correo(), s.producto(),
                s.cantidad(), s.total(), LocalDateTime.now().toString());
        byte[] json = Broker.JSON.writeValueAsBytes(pedido);

        // el canal no es thread-safe y el servidor HTTP usa varios threads
        synchronized (canal) {
            canal.basicPublish(Broker.EX_PEDIDOS, Broker.RK_PEDIDO_CREADO, Broker.propiedades(0), json);
        }
        Bitacora.registrar("OrderProducer", id, "pedido publicado en " + Broker.EX_PEDIDOS);
        return id;
    }

    private static void responder(HttpExchange intercambio, int codigo, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        intercambio.getResponseHeaders().add("Content-Type", "application/json");
        intercambio.sendResponseHeaders(codigo, bytes.length);
        try (OutputStream salida = intercambio.getResponseBody()) {
            salida.write(bytes);
        }
    }
}
