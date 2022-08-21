using Envelope.Database.PostgreSql;
using Envelope.Transactions;
using Npgsql.Internal;
using System.Reflection;

namespace Npgsql;

public static class NpgsqlConnectionExtensions
{
	private static readonly Lazy<Func<NpgsqlConnection, NpgsqlConnector>?> _connectorGetter = new(() =>
	{
		var type = typeof(NpgsqlConnection);
		var connectorProperty = type.GetProperty("Connector", BindingFlags.Instance | BindingFlags.NonPublic);
		if (connectorProperty == null)
			return null;

		var getter = Envelope.Reflection.Internal.DelegateFactory.CreateGet<NpgsqlConnection, NpgsqlConnector>(connectorProperty!);
		return getter!;
	});

	private static readonly Lazy<Func<NpgsqlConnector, bool>?> _inTransactionGetter = new(() =>
	{
		var type = typeof(NpgsqlConnector);
		var inTransactionProperty = type.GetProperty("InTransaction", BindingFlags.Instance | BindingFlags.NonPublic);
		if (inTransactionProperty == null)
			return null;

		var getter = Envelope.Reflection.Internal.DelegateFactory.CreateGet<NpgsqlConnector, bool>(inTransactionProperty!);
		return getter!;
	});

	private static readonly Lazy<Func<NpgsqlConnector, NpgsqlTransaction?>?> _transactionGetter = new(() =>
	{
		var type = typeof(NpgsqlConnector);
		var transactionProperty = type.GetProperty("Transaction", BindingFlags.Instance | BindingFlags.NonPublic);
		if (transactionProperty == null)
			return null;

		var getter = Envelope.Reflection.Internal.DelegateFactory.CreateGet<NpgsqlConnector, NpgsqlTransaction?>(transactionProperty);
		return getter!;
	});

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

	public static ITransactionCoordinator BeginTransactionAndAttachToTransactionCoordinator(this NpgsqlConnection connection, ITransactionCoordinator transactionCoordinator)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		if (transactionCoordinator == null)
			throw new ArgumentNullException(nameof(transactionCoordinator));

		var transaction = connection.BeginTransaction();
		transaction.AttachToTransactionCoordinator(transactionCoordinator);
		return transactionCoordinator;
	}

	public static ITransactionCoordinator BeginTransactionCoordinator(
		this NpgsqlConnection connection,
		IServiceProvider serviceProvider)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var transaction = connection.BeginTransaction();
		var transactionCoordinator = transaction.ToTransactionCoordinator(serviceProvider);
		return transactionCoordinator;
	}

#if NET6_0_OR_GREATER
	public static async Task<NpgsqlTransaction> BeginTransactionAndAttachToTransactionCoordinatorAsync(this NpgsqlConnection connection, ITransactionCoordinator transactionCoordinator, CancellationToken cancellationToken = default)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		if (transactionCoordinator == null)
			throw new ArgumentNullException(nameof(transactionCoordinator));

		var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
		transaction.AttachToTransactionCoordinator(transactionCoordinator);
		return transaction;
	}

	public static async Task<ITransactionCoordinator> BeginTransactionCoordinatorAsync(
		this NpgsqlConnection connection,
		IServiceProvider serviceProvider,
		CancellationToken cancellationToken = default)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
		var transactionCoordinator = transaction.ToTransactionCoordinator(serviceProvider);
		return transactionCoordinator;
	}
#endif

	public static bool IsInTransaction(this NpgsqlConnection connection)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var getter = _connectorGetter.Value;
		if (getter == null)
			return false;

		var connector = getter(connection);
		if (connector == null)
			return false;

		return connector?.IsInTransaction() ?? false;
	}

	public static bool IsInTransaction(this NpgsqlConnector connector)
	{
		if (connector == null)
			throw new ArgumentNullException(nameof(connector));

		var getter = _inTransactionGetter.Value;
		if (getter == null)
			return false;

		var result = getter(connector);
		return result;
	}

	public static NpgsqlTransaction? GetCurrentTransaction(this NpgsqlConnection connection)
	{
		if (connection == null)
			throw new ArgumentNullException(nameof(connection));

		var getter = _connectorGetter.Value;
		if (getter == null)
			return null;

		var connector = getter(connection);
		if (connector == null)
			return null;

		return connector?.GetCurrentTransaction();
	}

	//private static readonly object _getCurrentTransactionLock = new();
	public static NpgsqlTransaction? GetCurrentTransaction(this NpgsqlConnector connector)
	{
		if (connector == null)
			throw new ArgumentNullException(nameof(connector));

		NpgsqlTransaction? result;
		//lock (_getCurrentTransactionLock)
		//{
		var isInTransaction = connector.IsInTransaction();
		if (!isInTransaction)
			return null;

		var getter = _transactionGetter.Value;
		if (getter == null)
			return null;

		result = getter(connector);
		//}

		return result;
	}
}
