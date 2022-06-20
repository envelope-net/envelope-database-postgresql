using NpgsqlTypes;

namespace Envelope.Database.PostgreSql;

public interface IDictionaryTableOptions
{
	string? SchemaName { get; }
	string? TableName { get; }
	bool IsTemporaryTable { get; }
	List<string>? PropertyNames { get; }
	Dictionary<string, string>? PropertyColumnMapping { get; }
	Dictionary<string, NpgsqlDbType>? PropertyTypeMapping { get; }
	Dictionary<string, Func<object?, object?>>? PropertyValueConverter { get; }
	bool UseQuotationMarksForTableName { get; }
	bool UseQuotationMarksForColumnNames { get; }
	bool PropertyTypeMappingIsRequired { get; internal set; }

	void Validate(bool validateProperties, bool validatePropertyMapping);
}
