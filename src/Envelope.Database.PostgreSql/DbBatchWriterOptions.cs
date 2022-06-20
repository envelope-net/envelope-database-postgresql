using Envelope.Data;

namespace Envelope.Database.PostgreSql;

public class DbBatchWriterOptions : DictionaryTableOptions, IDbBatchWriterOptions, IDictionaryTableOptions, IBatchWriterOptions
{
	public string? ConnectionString { get; set; }

	public bool EagerlyEmitFirstEvent { get; set; } = true;

	public int BatchSizeLimit { get; set; } = 1000;

	public TimeSpan Period { get; set; } = TimeSpan.FromSeconds(2);

	public TimeSpan MinimumBackoffPeriod { get; set; } = TimeSpan.FromSeconds(5);

	public TimeSpan MaximumBackoffInterval { get; set; } = TimeSpan.FromMinutes(10);

	public int? QueueLimit { get; set; } = 100000;

	public DbBatchWriterOptions()
	{
	}

	public override void Validate(bool validateProperties, bool validatePropertyMapping)
	{
		if (string.IsNullOrWhiteSpace(ConnectionString))
			throw new InvalidOperationException($"{nameof(ConnectionString)} == null");

		base.Validate(validateProperties, validatePropertyMapping);
	}
}
