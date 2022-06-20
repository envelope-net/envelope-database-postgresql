using NpgsqlTypes;
using Envelope.Data;

namespace Envelope.Database.PostgreSql;

public interface IDbBatchWriterOptions : IDictionaryTableOptions, IBatchWriterOptions
{
	string? ConnectionString { get; set; }
}
