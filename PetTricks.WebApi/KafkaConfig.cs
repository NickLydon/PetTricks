namespace PetTricks.WebApi;

public class KafkaConfig
{
    public string BootstrapServers { get; set; }
    public string NewMoviesTopic { get; set; }
    public string LatestMediaPositionsTopic { get; set; }
}