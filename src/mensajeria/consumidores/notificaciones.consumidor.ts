import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConsumeMessage } from 'amqplib';
import { MensajeriaService } from '../mensajeria.service.js';
import { COLAS, EXCHANGES, ROUTING_KEYS } from '../topologia.js';

@Injectable()
export class NotificacionesConsumidor implements OnModuleInit {
  private readonly log = new Logger(NotificacionesConsumidor.name);

  constructor(private readonly mensajeria: MensajeriaService) {}

  async onModuleInit(): Promise<void> {
    const canal = this.mensajeria.canal;

    await canal.consume(
      COLAS.notificaciones,
      (mensaje: ConsumeMessage | null) => {
        if (!mensaje) return;

        const eventoId = mensaje.properties.headers?.['x-evento-id'];
        const emitidoEn = mensaje.properties.headers?.['x-emitido-en'];
        const payload = JSON.parse(mensaje.content.toString());

        this.log.log(
          `[Notificacion] ${mensaje.fields.routingKey} | evento ${eventoId} | usuario: ${payload.usuarioSub} | libro: ${payload.libroId}`,
        );

        const comandoCorreo = {
          destinatario: payload.usuarioSub,
          asunto: 'Prestamo registrado',
          libroId: payload.libroId,
          prestamoId: payload.prestamoId,
        };
        canal.publish(
          EXCHANGES.comandos.nombre,
          ROUTING_KEYS.correoEnviar,
          Buffer.from(JSON.stringify(comandoCorreo)),
          { contentType: 'application/json' },
        );

        canal.ack(mensaje);
      },
      { noAck: false },
    );

    this.log.log(`escuchando la cola ${COLAS.notificaciones}`);
  }
}
