using Envelope.Database.PostgreSql;
using Envelope.Transactions;

namespace Npgsql;

public static class NpgsqlTransactionExtensions
{
	public static ITransactionManager AttachToTransactionManager(this NpgsqlTransaction transaction, ITransactionManager transactionManager)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (transactionManager == null)
			throw new ArgumentNullException(nameof(transactionManager));

		transactionManager.ConnectTransactionObserver(new NpgsqlTransactionBehaviorObserver(transaction));

		transactionManager.AddUniqueItem(nameof(NpgsqlTransaction), transaction);
		transactionManager.AddUniqueItem(nameof(NpgsqlConnection), transaction.Connection);
		return transactionManager;
	}

	public static ITransactionManager ToTransactionManager(this NpgsqlTransaction transaction)
		=> ToTransactionManager(transaction, null);

	public static ITransactionManager ToTransactionManager(this NpgsqlTransaction transaction, Action<ITransactionObserverConnector>? configure)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		var transactionManager = TransactionManagerFactory.CreateTransactionManager(null, configure);
		return AttachToTransactionManager(transaction, transactionManager);
	}
}
