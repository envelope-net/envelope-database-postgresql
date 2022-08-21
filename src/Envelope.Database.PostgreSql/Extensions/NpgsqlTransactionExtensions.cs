using Envelope.Database;
using Envelope.Database.PostgreSql;
using Envelope.Transactions;

namespace Npgsql;

public static class NpgsqlTransactionExtensions
{
	public static ITransactionCoordinator AttachToTransactionCoordinator(
		this NpgsqlTransaction transaction,
		ITransactionCoordinator transactionCoordinator)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (transactionCoordinator == null)
			throw new ArgumentNullException(nameof(transactionCoordinator));

		transactionCoordinator.ConnectTransactionObserver(new NpgsqlTransactionBehaviorObserver(transaction));

		transactionCoordinator.AddUniqueItem(nameof(NpgsqlTransaction), transaction);
		transactionCoordinator.AddUniqueItem(nameof(NpgsqlConnection), transaction.Connection);
		return transactionCoordinator;
	}

	public static ITransactionCoordinator ToTransactionCoordinator(
		this NpgsqlTransaction transaction,
		IServiceProvider serviceProvider)
	{
		if (transaction == null)
			throw new ArgumentNullException(nameof(transaction));

		if (serviceProvider == null)
			throw new ArgumentNullException(nameof(serviceProvider));

		var transactionCoordinator = serviceProvider.GetService(typeof(ITransactionCoordinator)) as ITransactionCoordinator;
		if (transactionCoordinator == null)
			throw new InvalidOperationException($"{nameof(transactionCoordinator)} == null");

		return AttachToTransactionCoordinator(transaction, transactionCoordinator);
	}
}
