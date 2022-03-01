namespace PetTricks.WebApi;

public record Media(string Title, string[] Genres, TimeSpan Runtime)
{
    public virtual bool Equals(Media? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return string.Equals(Title, other.Title, StringComparison.OrdinalIgnoreCase) &&
               Genres.SequenceEqual(other.Genres, StringComparer.OrdinalIgnoreCase) &&
               Runtime.Equals(other.Runtime);
    }

    public override int GetHashCode() =>
        HashCode.Combine(
            Title.ToUpperInvariant(),
            Genres.Select(_ => _.ToUpperInvariant()).Aggregate(0, HashCode.Combine),
            Runtime);
}