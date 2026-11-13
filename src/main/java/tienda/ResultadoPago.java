package tienda;

/** Resultado final del cobro; lo publica PaymentWorker y lo lee EmailWorker. */
public record ResultadoPago(
        String pedidoId,
        String cliente,
        String correo,
        String paymentStatus,   // PAID o FAILED
        int intentos) {}
