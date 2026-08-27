using RabbitMqExample.Api.Messaging;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

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

// La API conserva una conexión con RabbitMQ y crea un canal aislado por llamada RPC.
builder.Services.AddSingleton<RabbitMqRpcClient>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<RabbitMqRpcClient>());
builder.Services.AddControllers();

WebApplication app = builder.Build();
app.MapControllers();
app.Run();
