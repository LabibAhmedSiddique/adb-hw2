import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Homework 2, Part 4: restart aborted transactions WITHOUT starvation.
 *
 * Based on Blocking.java by Martin Schaeler (martin.schaeler@plus.ac.at)
 * and our RestartBlocking.java (Part 3).
 *
 * Problem (Part 3): PostgreSQL picks deadlock victims without remembering
 * past losses. A restarted transaction is a brand-new transaction with no
 * priority, so the same task can be aborted again and again (starvation).
 *
 * Concept: the client remembers how often each task lost.
 *  - Normal mode: tasks run concurrently (shared "read" lock on a client-side
 *    gate). After an abort they wait a short random time (backoff + jitter)
 *    so they do not crash into the same rivals again immediately.
 *  - Exclusive mode: a task that was aborted MAX_SHARED_ATTEMPTS times takes
 *    the exclusive "write" lock of the gate. It waits until the running
 *    attempts are finished (committed or aborted), and no new attempt can
 *    start meanwhile. It then runs ALONE, so it cannot be part of a deadlock
 *    cycle and is guaranteed to commit.
 *  - The gate is FAIR: a waiting exclusive task cannot be overtaken forever
 *    by new normal tasks.
 *
 * Result: every task needs at most MAX_SHARED_ATTEMPTS + 1 attempts.
 *
 * Usage: java NoLiveLocks [tasks] [queries per task] [threads]
 */
public class NoLiveLocks {
	public static int num_taks = 30;
	public static int num_queries_per_task = 3;
	public static int num_threads = 4;

	/** After this many aborts a task switches to exclusive mode. */
	public static final int MAX_SHARED_ATTEMPTS = 3;
	/** Backoff after the k-th abort: random time in [0, BASE * 2^(k-1)] ms. */
	public static final int BACKOFF_BASE_MS = 50;

	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished = new AtomicInteger(0);
	AtomicInteger total_restarts = new AtomicInteger(0);
	AtomicInteger exclusive_runs = new AtomicInteger(0);
	AtomicInteger max_attempts = new AtomicInteger(0);
	AtomicInteger max_attempts_task = new AtomicInteger(-1);

	/** Client-side gate: many normal attempts at once, OR one exclusive attempt. */
	final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true); // true = fair

	void executeSQL(String sql, Connection con) {
		try {
			java.sql.Statement stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing " + sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	void cerateDB() {
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

			con.setAutoCommit(false);
			PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",
					ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			for (int i = 0; i < num_tuple; i++) {
				ps.setInt(1, i);
				ps.setDouble(2, rand.nextDouble() * max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}
			int[] results = ps.executeBatch();
			con.commit();
			con.close();
			System.out.println("Inserted " + results.length + " tuples");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/** Errors where trying the same transaction again can succeed. */
	static boolean isRetryable(SQLException e) {
		String state = e.getSQLState();
		return "40P01".equals(state)   // deadlock_detected
			|| "40001".equals(state);  // serialization_failure
	}

	static void rollbackQuietly(Connection con) {
		try {
			con.rollback();
		} catch (SQLException ignored) {
		}
	}

	static String firstLine(String msg) {
		if (msg == null) return "";
		int nl = msg.indexOf('\n');
		return nl < 0 ? msg : msg.substring(0, nl);
	}

	/** Exponential backoff with jitter: random wait in [0, BASE * 2^(aborts-1)] ms. */
	static void backoff(int aborts) {
		long max = (long) BACKOFF_BASE_MS << Math.min(aborts - 1, 10);
		try {
			Thread.sleep(ThreadLocalRandom.current().nextLong(max + 1));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public static void main(String[] args) {
		if (args.length > 0) num_taks = Integer.parseInt(args[0]);
		if (args.length > 1) num_queries_per_task = Integer.parseInt(args[1]);
		if (args.length > 2) num_threads = Integer.parseInt(args[2]);
		System.out.println("Tasks: " + num_taks + ", queries per task: " + num_queries_per_task
				+ ", threads: " + num_threads + ", exclusive after " + MAX_SHARED_ATTEMPTS + " aborts");

		NoLiveLocks b = new NoLiveLocks();
		b.cerateDB();

		ArrayList<NoLiveLocks.Blocker> my_tasks = new ArrayList<NoLiveLocks.Blocker>(num_taks);
		for (int task = 0; task < num_taks; task++) {
			my_tasks.add(b.new Blocker(task));
		}

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
		double start = System.currentTimeMillis();
		for (Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		double stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: "
				+ b.tasks_finished + " of " + num_taks);
		System.out.println("[STATS] Total restarts: " + b.total_restarts
				+ ", exclusive runs: " + b.exclusive_runs
				+ ", most attempts by one task: " + b.max_attempts + " (task " + b.max_attempts_task + ")");
	}

	public class Blocker implements Runnable {
		final int task;
		final double[] start_range;
		final double[] stop_range;

		public Blocker(int id) {
			this.task = id;
			this.start_range = new double[num_queries_per_task];
			this.stop_range = new double[num_queries_per_task];
			for (int q = 0; q < num_queries_per_task; q++) {
				stop_range[q] = rand.nextDouble() * max_value;
				start_range[q] = rand.nextDouble() * stop_range[q];
			}
		}

		@Override
		public void run() {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			int attempts = 0;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				PreparedStatement ps = con.prepareStatement(
						"UPDATE blocking_data SET data_value = ?,  modified_by = " + task
								+ " WHERE data_value >= ? AND data_value <= ?;",
						ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

				boolean done = false;
				while (!done) {
					attempts++;
					// Lost too often? Then this attempt runs alone.
					boolean exclusive = attempts > MAX_SHARED_ATTEMPTS;
					Lock lock = exclusive ? gate.writeLock() : gate.readLock();
					if (exclusive) {
						exclusive_runs.incrementAndGet();
						System.out.println("Task " + task + " starving -> running EXCLUSIVELY (attempt " + attempts + ")");
					}

					lock.lock(); // exclusive: waits until all running attempts are finished
					try {
						for (int q = 0; q < num_queries_per_task; q++) {
							ps.setDouble(1, (float) (rand.nextDouble() * max_value));
							ps.setDouble(2, start_range[q]);
							ps.setDouble(3, stop_range[q]);
							ps.execute();
						}
						con.commit();
						done = true; // only AFTER the commit succeeded
					} catch (SQLException e) {
						rollbackQuietly(con); // release all DB locks BEFORE leaving the gate
						if (!isRetryable(e)) {
							throw e;
						}
						total_restarts.incrementAndGet();
						System.out.println("Task " + task + " aborted on attempt " + attempts
								+ ", restarting. Reason: " + firstLine(e.getMessage()));
					} finally {
						lock.unlock(); // always leave the gate, success or failure
					}

					if (!done) {
						backoff(attempts); // outside the gate: do not block others while sleeping
					}
				}

				int num_finished = tasks_finished.incrementAndGet();
				recordAttempts(attempts);
				System.out.println("Done task " + task + " after " + attempts + " attempt(s). I am finisher number " + num_finished);

			} catch (SQLException e) {
				System.err.println("Task " + task + " failed permanently: " + e.getMessage());
			} finally {
				try {
					con.close();
				} catch (SQLException ignored) {
				}
			}
		}

		private void recordAttempts(int attempts) {
			synchronized (NoLiveLocks.this) {
				if (attempts > max_attempts.get()) {
					max_attempts.set(attempts);
					max_attempts_task.set(task);
				}
			}
		}
	}
}
