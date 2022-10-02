using Envelope.Extensions;
using Envelope.Transactions;
using Npgsql;

namespace Envelope.Database.PostgreSql;

internal class NpgsqlTransactionBehaviorObserver : ITransactionBehaviorObserver
{
	private bool _disposed;
	private readonly NpgsqlTransaction _transaction;
	private readonly int _waitForConnectionExecutingInMilliseconds;
	private readonly int _waitForConnectionExecutingCount;

	public NpgsqlTransactionBehaviorObserver(NpgsqlTransaction transaction, int waitForConnectionExecutingInMilliseconds = 50, int waitForConnectionExecutingCount = 40)
	{
		_transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
		_waitForConnectionExecutingInMilliseconds = waitForConnectionExecutingInMilliseconds;
		_waitForConnectionExecutingCount = waitForConnectionExecutingCount;
	}

	public void Commit(ITransactionCoordinator transactionCoordinator)
	{
		if (_transaction.IsCompletedTransaction() || _transaction.IsDisposedTransaction())
			return;

		var iterations = 0;
		while (_transaction.Connection?.State == System.Data.ConnectionState.Executing)
		{
			iterations++;
			Thread.Sleep(_waitForConnectionExecutingInMilliseconds);
			if (_waitForConnectionExecutingCount < iterations)
				break;
		}

		if (!_transaction.IsCompletedTransaction() && !_transaction.IsDisposedTransaction())
			_transaction.Commit();
	}

	public async Task CommitAsync(ITransactionCoordinator transactionCoordinator, CancellationToken cancellationToken)
	{
		if (_transaction.IsCompletedTransaction() || _transaction.IsDisposedTransaction())
			return;

		var iterations = 0;
		while (_transaction.Connection?.State == System.Data.ConnectionState.Executing)
		{
			iterations++;
			await Task.Delay(_waitForConnectionExecutingInMilliseconds, cancellationToken);
			if (_waitForConnectionExecutingCount < iterations)
				break;
		}

		if (!_transaction.IsCompletedTransaction() && !_transaction.IsDisposedTransaction())
			await _transaction.CommitAsync(cancellationToken);
	}

	public void Rollback(ITransactionCoordinator transactionCoordinator, Exception? exception)
	{
		if (_transaction.IsCompletedTransaction() || _transaction.IsDisposedTransaction())
			return;

		var iterations = 0;
		while (_transaction.Connection?.State == System.Data.ConnectionState.Executing)
		{
			iterations++;
			Thread.Sleep(_waitForConnectionExecutingInMilliseconds);
			if (_waitForConnectionExecutingCount < iterations)
				break;
		}

		if (!_transaction.IsCompletedTransaction() && !_transaction.IsDisposedTransaction())
			_transaction.Rollback();
	}

	public async Task RollbackAsync(ITransactionCoordinator transactionCoordinator, Exception? exception, CancellationToken cancellationToken)
	{
		if (_transaction.IsCompletedTransaction() || _transaction.IsDisposedTransaction())
			return;

		var iterations = 0;
		while (_transaction.Connection?.State == System.Data.ConnectionState.Executing)
		{
			iterations++;
			await Task.Delay(_waitForConnectionExecutingInMilliseconds, cancellationToken);
			if (_waitForConnectionExecutingCount < iterations)
				break;
		}

		if (!_transaction.IsCompletedTransaction() && !_transaction.IsDisposedTransaction())
			await _transaction.RollbackAsync(cancellationToken);
	}

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
