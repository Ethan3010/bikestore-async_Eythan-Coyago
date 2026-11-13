package tienda;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Registro de eventos en consola: fecha y hora, componente, pedidoId y thread. */
public final class Bitacora {

    private Bitacora() {}

    private static final DateTimeFormatter FORMATO = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static void registrar(String componente, String pedidoId, String texto) {
        System.out.printf("%s | %-13s | pedidoId=%s | thread=%s | %s%n",
                LocalDateTime.now().format(FORMATO), componente, pedidoId,
                Thread.currentThread().getName(), texto);
    }

    public static void pausa(long milisegundos) {
        try {
            Thread.sleep(milisegundos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
