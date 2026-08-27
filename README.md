# RabbitMQ RPC básico con .NET 10

Ejemplo de comunicación RPC mediante RabbitMQ. Una API publica una solicitud en la cola `weather` y espera la respuesta generada por una aplicación de consola.

## Proyectos

| Proyecto | Responsabilidad |
| --- | --- |
| `RabbitMqExample.Api` | Expone el endpoint HTTP y actúa como cliente RPC. |
| `RabbitMqExample.Consumer` | Consume las solicitudes y publica las respuestas. |

## Ejecutar

Requisitos: .NET SDK 10, Visual Studio y Docker Desktop.

Inicia RabbitMQ desde la raíz de la solución:

```powershell
docker compose up -d
```

Selecciona el perfil de solución `API + consumidor` y ejecuta la solución. Visual Studio inicia `RabbitMqExample.Api` y `RabbitMqExample.Consumer`.

Cuando ambos proyectos estén conectados a RabbitMQ, abre `RabbitMqExample.Api/RabbitMqExample.Api.http` y ejecuta `GET /weatherforecast`.

Resultado esperado:

- La API publica una solicitud RPC en la cola `weather`.
- El consumidor genera cinco pronósticos y envía la respuesta.
- La API relaciona la respuesta mediante `CorrelationId` y devuelve `200 OK`.

La petición HTTP permanece esperando hasta que el consumidor responde.

## RabbitMQ Management

El panel está disponible en `http://localhost:15672` con las credenciales `guest` / `guest`.

Para detener RabbitMQ:

```powershell
docker compose down
```

El ejemplo avanzado está en [RabbitMQ-Ejemplo-Avanzado](https://github.com/sergioocode/RabbitMQ-Ejemplo-Avanzado).
