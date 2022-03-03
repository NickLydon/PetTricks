using PetTricks.WebApi;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.Configure<KafkaConfig>(builder.Configuration.GetSection(nameof(KafkaConfig)));
builder.Services.AddSingleton<KafkaClientHandle>();
builder.Services.AddSingleton<KafkaDependentProducer<Guid, Media>>();
builder.Services.AddSingleton<KafkaDependentProducer<Guid, LatestMediaPosition>>();
builder.Services.AddSingleton<KafkaDependentProducer<Guid, User>>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
