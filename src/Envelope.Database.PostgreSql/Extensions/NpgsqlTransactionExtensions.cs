using Envelope.Database.PostgreSql;
using Envelope.Transactions;

namespace Npgsql;

public static class NpgsqlTransactionExtensions
{
	public static ITransactionContext AttachToTransactionContext(this NpgsqlTransaction transaction, ITransactionContext transactionContext)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (transactionContext == null)
			throw new ArgumentNullException(nameof(transactionContext));

		transactionContext.ConnectTransactionManager(new NpgsqlTransactionBehaviorObserver(transaction));

		transactionContext.AddUniqueItem(nameof(NpgsqlTransaction), transaction);
		transactionContext.AddUniqueItem(nameof(NpgsqlConnection), transaction.Connection);
		return transactionContext;
	}

	public static ITransactionContext ToTransactionContext(this NpgsqlTransaction transaction)
		=> ToTransactionContext(transaction, null);

	public static ITransactionContext ToTransactionContext(this NpgsqlTransaction transaction, Action<ITransactionObserverConnector>? configure)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		var transactionContext = TransactionContextFactory.CreateTransactionContext(null, configure);
		return AttachToTransactionContext(transaction, transactionContext);
	}
}
