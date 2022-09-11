using Envelope.Transactions;
using Npgsql;

namespace Envelope.Database.PostgreSql;

internal class NpgsqlTransactionBehaviorObserver : ITransactionBehaviorObserver
{
	private bool _disposed;
	private readonly NpgsqlTransaction _transaction;

	public NpgsqlTransactionBehaviorObserver(NpgsqlTransaction transaction)
	{
		_transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
	}

	public void Commit(ITransactionCoordinator transactionCoordinator)
		=> _transaction.Commit();

	public Task CommitAsync(ITransactionCoordinator transactionCoordinator, CancellationToken cancellationToken)
		=> _transaction.CommitAsync(cancellationToken);

	public void Rollback(ITransactionCoordinator transactionCoordinator, Exception? exception)
		=> _transaction.Rollback();

	public Task RollbackAsync(ITransactionCoordinator transactionCoordinator, Exception? exception, CancellationToken cancellationToken)
		=> _transaction.RollbackAsync(cancellationToken);

	public async ValueTask DisposeAsync()
	{
		if (_disposed)
			return;

		_disposed = true;

		await DisposeAsyncCoreAsync().ConfigureAwait(false);

		Dispose(disposing: false);
		GC.SuppressFinalize(this);
	}

	protected virtual ValueTask DisposeAsyncCoreAsync()
		=> _transaction.DisposeAsync();

	protected virtual void Dispose(bool disposing)
	{
		if (_disposed)
			return;

		_disposed = true;

		if (disposing)
			_transaction.Dispose();
	}

	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
