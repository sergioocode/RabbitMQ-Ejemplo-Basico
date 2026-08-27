namespace RabbitMqExample.Consumer;

/// <summary>
/// Contrato que el consumidor serializa como respuesta y envía a través de RabbitMQ.
/// </summary>
public sealed class WeatherForecast
{
    public DateOnly Date { get; set; }

    public int TemperatureC { get; set; }

    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);

    public string? Summary { get; set; }
}
