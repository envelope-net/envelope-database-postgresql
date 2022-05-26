using Envelope.Transactions;
using Npgsql;

namespace Envelope.Database.PostgreSql;

internal class NpgsqlTransactionBehaviorObserver : ITransactionBehaviorObserver
{
	private readonly NpgsqlTransaction _transaction;

	public NpgsqlTransactionBehaviorObserver(NpgsqlTransaction transaction)
	{
		_transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
	}

	public void Commit(ITransactionContext transactionContext)
		=> _transaction.Commit();

	public Task CommitAsync(ITransactionContext transactionContext, CancellationToken cancellationToken)
		=> _transaction.CommitAsync(cancellationToken);

	public void Rollback(ITransactionContext transactionContext, Exception? exception)
		=> _transaction.Rollback();

	public Task RollbackAsync(ITransactionContext transactionContext, Exception? exception, CancellationToken cancellationToken)
		=> _transaction.RollbackAsync(cancellationToken);

	public ValueTask DisposeAsync()
		=> _transaction.DisposeAsync();

	public void Dispose()
		=> _transaction.Dispose();
}
