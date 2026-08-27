using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using RabbitMqExample.Api.Messaging;

namespace RabbitMqExample.Api.Controllers;

/// <summary>
/// Punto de entrada HTTP. Delega el trabajo en el microservicio de consola
/// mediante una petición RPC transportada por RabbitMQ.
/// </summary>
[ApiController]
[Route("[controller]")]
public sealed class WeatherForecastController(
    ILogger<WeatherForecastController> logger,
    RabbitMqRpcClient rpcClient
) : ControllerBase
{
    [HttpGet]
    public async Task<ActionResult<JsonElement>> Get(CancellationToken cancellationToken)
    {
        // Paso 1: el cliente llama por HTTP a GET /weatherforecast.
        logger.LogInformation("Received request for weather forecast.");

        // Paso 2: el cliente RPC publica la petición en la cola duradera "weather".
        // ReplyTo identifica una cola temporal exclusiva y CorrelationId relaciona
        // la respuesta con esta llamada HTTP concreta.
        //
        // El await permanece esperando mientras Consumer procesa el mensaje.
        JsonElement result = await rpcClient.CallAsync("weather", cancellationToken);

        // Paso 6: el cliente recibió la respuesta de la cola temporal, comprobó
        // su CorrelationId y completó el await. Ahora la API devuelve HTTP 200.
        return Ok(result);
    }
}
