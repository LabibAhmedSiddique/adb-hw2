import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


public class RestartBlocking {
	public static int num_taks = 30;
	public static int num_queries_per_task = 3;
	public static int num_threads = 4;

	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger total_restarts;
	AtomicInteger max_attempts;
	AtomicInteger max_attempts_task;

	public RestartBlocking() {
		tasks_finished = new AtomicInteger(0);
		total_restarts = new AtomicInteger(0);
		max_attempts = new AtomicInteger(0);
		max_attempts_task = new AtomicInteger(-1);
	}

	void executeSQL(String sql, Connection con) {
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
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

			PreparedStatement ps;
			con.setAutoCommit(false);
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

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

	/** Roll back, but never let a failing rollback crash the task. */
	static void rollbackQuietly(Connection con) {
		try {
			con.rollback();
		} catch (SQLException ignored) {
			// connection may already be broken; nothing more we can do
		}
	}

	/** Postgres messages span several lines; the first line is enough for logs. */
	static String firstLine(String msg) {
		if (msg == null) return "";
		int nl = msg.indexOf('\n');
		return nl < 0 ? msg : msg.substring(0, nl);
	}

	public static void main(String[] args) {
		if (args.length > 0) num_taks = Integer.parseInt(args[0]);
		if (args.length > 1) num_queries_per_task = Integer.parseInt(args[1]);
		if (args.length > 2) num_threads = Integer.parseInt(args[2]);
		System.out.println("Tasks: " + num_taks + ", queries per task: " + num_queries_per_task + ", threads: " + num_threads);

		// Create DB
		RestartBlocking b = new RestartBlocking();
		b.cerateDB();

		// Create tasks and their queries
		ArrayList<RestartBlocking.Blocker> my_tasks = new ArrayList<RestartBlocking.Blocker>(num_taks);
		for (int task = 0; task < num_taks; task++) {
			my_tasks.add(b.new Blocker(task));
		}

		// Execute tasks
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
		double start, stop;
		start = System.currentTimeMillis();
		for (Blocker task : my_tasks) {
			executor.execute(task);
		}
		executor.shutdown();

		// Wait for the tasks to finish
		while (!executor.isTerminated()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		stop = System.currentTimeMillis();
		System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: "
				+ b.tasks_finished + " of " + num_taks);
		System.out.println("[STATS] Total restarts: " + b.total_restarts
				+ ", most attempts by one task: " + b.max_attempts + " (task " + b.max_attempts_task + ")");
	}

	/**
	 * A task holding many write locks for a long time; now restarts itself
	 * whenever the DBMS aborts it because of a deadlock.
	 */
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
			// One connection per task (see original comment in Blocking.java)
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
					try {
						// The whole transaction: all queries, then commit
						for (int q = 0; q < num_queries_per_task; q++) {
							ps.setDouble(1, (float) (rand.nextDouble() * max_value));
							ps.setDouble(2, start_range[q]);
							ps.setDouble(3, stop_range[q]);
							ps.execute();
						}
						con.commit();
						done = true; // only AFTER the commit succeeded
					} catch (SQLException e) {
						rollbackQuietly(con); // release all our locks
						if (!isRetryable(e)) {
							throw e; // not a deadlock -> retrying will not help
						}
						total_restarts.incrementAndGet();
						System.out.println("Task " + task + " aborted on attempt " + attempts
								+ ", restarting. Reason: " + firstLine(e.getMessage()));
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

		/** Remember the task that needed the most attempts (thread-safe). */
		private void recordAttempts(int attempts) {
			synchronized (RestartBlocking.this) {
				if (attempts > max_attempts.get()) {
					max_attempts.set(attempts);
					max_attempts_task.set(task);
				}
			}
		}
	}
}
