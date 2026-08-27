using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMqExample.Consumer.Messaging;
using RabbitMqExample.Consumer.Services;

// Esta aplicación de consola es el microservicio consumidor.
// Host carga appsettings.json, configura logging y prepara la inyección de dependencias.
HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

builder
    .Services.AddOptions<RabbitMqOptions>()
    .Bind(builder.Configuration.GetSection(RabbitMqOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.HostName),
        "RabbitMQ:HostName es obligatorio."
    )
    .Validate(options => options.Port is > 0 and <= 65535, "RabbitMQ:Port no es válido.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.UserName),
        "RabbitMQ:UserName es obligatorio."
    )
    .ValidateOnStart();

builder.Services.AddSingleton<WeatherForecastService>();
builder.Services.AddHostedService<WeatherRpcWorker>();

await builder.Build().RunAsync();
