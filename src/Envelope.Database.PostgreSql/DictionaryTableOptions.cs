using NpgsqlTypes;

namespace Envelope.Database.PostgreSql;

public class DictionaryTableOptions
{
	public string? SchemaName { get; set; }
	public string? TableName { get; set; }
	public bool IsTemporaryTable { get; set; }
	public List<string>? PropertyNames { get; set; }
	public Dictionary<string, string>? PropertyColumnMapping { get; set; }
	public Dictionary<string, NpgsqlDbType>? PropertyTypeMapping { get; set; }
	public Dictionary<string, Func<object?, object?>>? PropertyValueConverter { get; set; }
	public bool UseQuotationMarksForTableName { get; set; } = true;
	public bool UseQuotationMarksForColumnNames { get; set; } = true;
	public bool PropertyTypeMappingIsRequired { get; set; }

	public DictionaryTableOptions Validate(bool validateProperties, bool validatePropertyMapping)
	{
		if (!IsTemporaryTable && string.IsNullOrWhiteSpace(SchemaName))
			throw new InvalidOperationException($"{nameof(SchemaName)} == null");

		if (string.IsNullOrWhiteSpace(TableName))
			throw new InvalidOperationException($"{nameof(TableName)} == null");

		if (validateProperties && (PropertyNames == null || PropertyNames.Count == 0))
			throw new InvalidOperationException($"{nameof(PropertyNames)} == null");

		if (PropertyNames != null)
			foreach (var propertyName in PropertyNames)
				if (string.IsNullOrWhiteSpace(propertyName))
					throw new InvalidOperationException($"{nameof(PropertyNames)}: NULL column name is not valid.");

		if (PropertyColumnMapping != null)
			foreach (var kvp in PropertyColumnMapping)
				if (string.IsNullOrWhiteSpace(kvp.Value))
					throw new InvalidOperationException($"{nameof(PropertyColumnMapping)}: Property {kvp.Key} has NULL column mapping.");

		if (validatePropertyMapping && (PropertyTypeMapping == null || PropertyTypeMapping.Count == 0))
			throw new InvalidOperationException($"{nameof(PropertyTypeMapping)} == null");

		return this;
	}
}
