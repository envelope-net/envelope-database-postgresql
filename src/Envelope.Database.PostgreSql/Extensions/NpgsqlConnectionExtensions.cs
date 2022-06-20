using Envelope.Database.PostgreSql;
using Envelope.Transactions;

namespace Npgsql;

public static class NpgsqlConnectionExtensions
{
	public static Task<bool> ExistsAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string tableName, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.ExistsAsync(connection, transaction, tableName, cancellationToken);

	public static Task<bool> ExistsAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string schemaName, string tableName, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.ExistsAsync(connection, transaction, schemaName, tableName, cancellationToken);

	public static Task<string> CopyTableAsTempIfNotExistsAsync(
		this NpgsqlConnection connection,
		NpgsqlTransaction? transaction,
		string sourceSchemaName,
		string sourceTableName,
		TmpTableCommitOptions commitOptions = TmpTableCommitOptions.PreserveRows,
		bool copyWithData = false,
		bool truncateDataIfAny = true,
		CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.CopyTableAsTempIfNotExistsAsync(connection, transaction, sourceSchemaName, sourceTableName, commitOptions, copyWithData, truncateDataIfAny, cancellationToken);

	public static Task<string> CopyTableAsTempIfNotExistsAsync(
		this NpgsqlConnection connection,
		NpgsqlTransaction? transaction,
		string sourceSchemaName,
		string sourceTableName,
		string tmpTableName,
		TmpTableCommitOptions commitOptions,
		bool copyWithData,
		bool truncateDataIfAny,
		CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.CopyTableAsTempIfNotExistsAsync(connection, transaction, sourceSchemaName, sourceTableName, tmpTableName, commitOptions, copyWithData, truncateDataIfAny, cancellationToken);

	public static Task TruncateTableAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string tableName, bool cascade, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.TruncateTableAsync(connection, transaction, tableName, cascade, cancellationToken);

	public static Task TruncateTableAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string schemaName, string tableName, bool cascade, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.TruncateTableAsync(connection, transaction, schemaName, tableName, cascade, cancellationToken);

	public static Task DropTableAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string schemaName, string tableName, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.DropTableAsync(connection, transaction, schemaName, tableName, cancellationToken);

	public static Task DropTableAsync(this NpgsqlConnection connection, NpgsqlTransaction? transaction, string tableName, CancellationToken cancellationToken = default)
		=> PostgreSqlCommands.DropTableAsync(connection, transaction, tableName, cancellationToken);

	public static ITransactionManager BeginTransactionAndAttachToTransactionManager(this NpgsqlConnection connection, ITransactionManager transactionManager)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		if (transactionManager == null)
			throw new ArgumentNullException(nameof(transactionManager));

		var transaction = connection.BeginTransaction();
		transaction.AttachToTransactionManager(transactionManager);
		return transactionManager;
	}

	public static ITransactionManager BeginTransactionManager(this NpgsqlConnection connection)
		=> BeginTransactionManager(connection, null);

	public static ITransactionManager BeginTransactionManager(this NpgsqlConnection connection, Action<ITransactionObserverConnector>? configure)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var transaction = connection.BeginTransaction();
		var transactionManager = transaction.ToTransactionManager(configure);
		return transactionManager;
	}

#if NET6_0_OR_GREATER
	public static async Task<NpgsqlTransaction> BeginTransactionAndAttachToTransactionManagerAsync(this NpgsqlConnection connection, ITransactionManager transactionManager, CancellationToken cancellationToken = default)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		if (transactionManager == null)
			throw new ArgumentNullException(nameof(transactionManager));

		var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
		transaction.AttachToTransactionManager(transactionManager);
		return transaction;
	}

	public static Task<ITransactionManager> BeginTransactionManagerAsync(this NpgsqlConnection connection, CancellationToken cancellationToken = default)
		=> BeginTransactionManagerAsync(connection, null, cancellationToken);

	public static async Task<ITransactionManager> BeginTransactionManagerAsync(this NpgsqlConnection connection, Action<ITransactionObserverConnector>? configure, CancellationToken cancellationToken = default)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
		var transactionManager = transaction.ToTransactionManager(configure);
		return transactionManager;
	}
#endif
}
