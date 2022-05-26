using NpgsqlTypes;
using Envelope.Extensions;
using System.Collections.Concurrent;

namespace Envelope.Database.PostgreSql;

public static class NpgsqlDbTypeHelper
{
	private static readonly Lazy<ConcurrentDictionary<Type, NpgsqlDbType>> _cache = new();

	public static NpgsqlDbType GetNpgsqlDbType<T>()
		where T : struct
	{
		var type = typeof(T).GetUnderlyingNullableType();

		if (_cache.Value.TryGetValue(type, out NpgsqlDbType result))
			return result;

		if (type == typeof(Guid))
			result = NpgsqlDbType.Uuid;
		else if (type == typeof(int))
			result = NpgsqlDbType.Integer;
		else if (type == typeof(long))
			result = NpgsqlDbType.Bigint;
		else if (type == typeof(bool))
			result = NpgsqlDbType.Boolean;
		else if (type == typeof(byte))
			result = NpgsqlDbType.Bytea;
		else if (type == typeof(char))
			result = NpgsqlDbType.Char;
		else if (type == typeof(decimal))
			result = NpgsqlDbType.Numeric;
		else if (type == typeof(double))
			result = NpgsqlDbType.Numeric;
		else if (type == typeof(float))
			result = NpgsqlDbType.Numeric;
		else if (type == typeof(short))
			result = NpgsqlDbType.Smallint;
		else if (type == typeof(DateTime))
			result = NpgsqlDbType.Timestamp;
		else if (type == typeof(DateTimeOffset))
			result = NpgsqlDbType.Timestamp;
#if NET6_0_OR_GREATER
		else if (type == typeof(DateOnly))
			result = NpgsqlDbType.Timestamp;
		else if (type == typeof(TimeOnly))
			result = NpgsqlDbType.Timestamp;
#endif
		else if (type == typeof(TimeSpan))
			result = NpgsqlDbType.Timestamp;
		else
			throw new NotSupportedException();

		_cache.Value.TryAdd(type, result);
		return result;
	}
}
