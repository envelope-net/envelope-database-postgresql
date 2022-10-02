using Envelope.Database.PostgreSql;
using Envelope.Transactions;
using Npgsql;
using System.Reflection;

namespace Envelope.Extensions;

public static class NpgsqlTransactionExtensions
{
	private static readonly Lazy<Func<NpgsqlTransaction, bool>?> _isDisposedGetter = new(() =>
	{
		var type = typeof(NpgsqlTransaction);
		var connectorProperty = type.GetProperty("IsDisposed", BindingFlags.Instance | BindingFlags.NonPublic);
		if (connectorProperty == null)
			return null;

		var getter = Envelope.Reflection.Internal.DelegateFactory.CreateGet<NpgsqlTransaction, bool>(connectorProperty!);
		return getter!;
	});

	private static readonly Lazy<Func<NpgsqlTransaction, bool>?> _isCompletedGetter = new(() =>
	{
		var type = typeof(NpgsqlTransaction);
		var connectorProperty = type.GetProperty("IsCompleted", BindingFlags.Instance | BindingFlags.NonPublic);
		if (connectorProperty == null)
			return null;

		var getter = Envelope.Reflection.Internal.DelegateFactory.CreateGet<NpgsqlTransaction, bool>(connectorProperty!);
		return getter!;
	});

	public static ITransactionCoordinator AttachToTransactionCoordinator(
		this NpgsqlTransaction transaction,
		ITransactionCoordinator transactionCoordinator,
		int waitForConnectionExecutingInMilliseconds = 50,
		int waitForConnectionExecutingCount = 40)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (transactionCoordinator == null)
			throw new ArgumentNullException(nameof(transactionCoordinator));

		transactionCoordinator.ConnectTransactionObserver(new NpgsqlTransactionBehaviorObserver(transaction, waitForConnectionExecutingInMilliseconds, waitForConnectionExecutingCount));

		transactionCoordinator.AddUniqueItem(nameof(NpgsqlTransaction), transaction);
		transactionCoordinator.AddUniqueItem(nameof(NpgsqlConnection), transaction.Connection);
		return transactionCoordinator;
	}

	public static ITransactionCoordinator ToTransactionCoordinator(
		this NpgsqlTransaction transaction,
		IServiceProvider serviceProvider,
		int waitForConnectionExecutingInMilliseconds = 50,
		int waitForConnectionExecutingCount = 40)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		if (serviceProvider.GetService(typeof(ITransactionCoordinator)) is not ITransactionCoordinator transactionCoordinator)
			throw new InvalidOperationException($"{nameof(transactionCoordinator)} == null");

		return AttachToTransactionCoordinator(transaction, transactionCoordinator, waitForConnectionExecutingInMilliseconds, waitForConnectionExecutingCount);
	}

	public static bool IsDisposedTransaction(this NpgsqlTransaction transaction)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		var getter = _isDisposedGetter.Value;
		if (getter == null)
			return false;

		var isDisposed = getter(transaction);
		return isDisposed;
	}

	public static bool IsCompletedTransaction(this NpgsqlTransaction transaction)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		var getter = _isCompletedGetter.Value;
		if (getter == null)
			return false;

		var isCompleted = getter(transaction);
		return isCompleted;
	}
}
