package tienda;

/** Mensaje JSON que publica OrderProducer. */
public record Pedido(
        String pedidoId,
        String cliente,
        String correo,
        String producto,
        int cantidad,
        double total,
        String creadoEn) {}
