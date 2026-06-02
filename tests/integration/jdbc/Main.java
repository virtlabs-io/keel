import java.net.URI;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * tests/integration/jdbc/Main.java — JDBC repro of prepared-statement load.
 *
 * Runs N iterations of two long-lived server-side PreparedStatements
 * (prepareThreshold=1):
 *   - INSERT ... RETURNING pg_current_wal_lsn(), txid_current()
 *   - SELECT payload WHERE test_id=? AND client_id=? AND seq=?
 *
 * Each iteration exercises Bind+Execute+Sync over the extended protocol on
 * named server-side prepared statements. This is the canonical Hibernate /
 * JPA / Spring Data load pattern and is the path that surfaces KEEL's
 * "failed to queue linked send+recv" regression after ~12 iterations.
 *
 * Exit codes (consumed by tests/integration/test_pg_jdbc_prepared.sh):
 *   0 — all iterations passed
 *   1 — at least one iteration produced an outcome != "pass"
 *   2 — bad arguments
 *
 * Stdout (text mode, default): one summary line:
 *   total=N passed=P stale=S ambiguous=A errors=E first_error_seq=K
 *
 * Stdout (--json mode): pretty JSON matching prophet.Summary.
 */
public class Main {

    // ---- JSON helpers ------------------------------------------------------

    static final DateTimeFormatter ISO8601 =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")
                         .withZone(ZoneOffset.UTC);

    static String jsonStr(String s) {
        if (s == null) s = "";
        return "\"" + s.replace("\\", "\\\\")
                       .replace("\"", "\\\"")
                       .replace("\n", "\\n")
                       .replace("\r", "\\r") + "\"";
    }

    static class IterationResult {
        final int    seq;
        final String payload;
        final String observed;
        final String commitLSN;
        final String txID;
        final boolean commitAck;
        final String outcome;
        final Instant occurredAt;
        final String error;

        IterationResult(int seq, String payload, String observed,
                        String commitLSN, String txID, boolean commitAck,
                        String outcome, Instant occurredAt, String error) {
            this.seq        = seq;
            this.payload    = payload    != null ? payload    : "";
            this.observed   = observed   != null ? observed   : "";
            this.commitLSN  = commitLSN  != null ? commitLSN  : "";
            this.txID       = txID       != null ? txID       : "";
            this.commitAck  = commitAck;
            this.outcome    = outcome;
            this.occurredAt = occurredAt;
            this.error      = error;
        }

        String toJson() {
            StringBuilder sb = new StringBuilder();
            sb.append("    {\n");
            sb.append("      \"seq\": ").append(seq).append(",\n");
            sb.append("      \"payload\": ").append(jsonStr(payload)).append(",\n");
            sb.append("      \"observed\": ").append(jsonStr(observed)).append(",\n");
            if (!commitLSN.isEmpty())
                sb.append("      \"commit_lsn\": ").append(jsonStr(commitLSN)).append(",\n");
            if (!txID.isEmpty())
                sb.append("      \"txid\": ").append(jsonStr(txID)).append(",\n");
            sb.append("      \"commit_ack\": ").append(commitAck).append(",\n");
            sb.append("      \"outcome\": ").append(jsonStr(outcome)).append(",\n");
            sb.append("      \"occurred_at\": ").append(jsonStr(ISO8601.format(occurredAt)));
            if (error != null && !error.isEmpty())
                sb.append(",\n      \"error\": ").append(jsonStr(error));
            sb.append("\n    }");
            return sb.toString();
        }
    }

    static class Summary {
        final int total;
        int passed, staleReads, ambiguous, errors;
        int firstErrorSeq = -1;
        final List<IterationResult> results;

        Summary(int total) {
            this.total   = total;
            this.results = new ArrayList<>(total);
        }

        void recordOutcome(IterationResult r) {
            results.add(r);
            switch (r.outcome) {
                case "pass":       passed++;     break;
                case "stale_read": staleReads++; break;
                case "ambiguous":  ambiguous++;  break;
                default:           errors++;
                                   if (firstErrorSeq == -1) firstErrorSeq = r.seq;
            }
        }

        String toJson() {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("  \"total\": ").append(total).append(",\n");
            sb.append("  \"passed\": ").append(passed).append(",\n");
            sb.append("  \"stale_reads\": ").append(staleReads).append(",\n");
            sb.append("  \"ambiguous\": ").append(ambiguous).append(",\n");
            sb.append("  \"errors\": ").append(errors).append(",\n");
            sb.append("  \"first_error_seq\": ").append(firstErrorSeq).append(",\n");
            sb.append("  \"results\": [\n");
            for (int i = 0; i < results.size(); i++) {
                sb.append(results.get(i).toJson());
                if (i < results.size() - 1) sb.append(",");
                sb.append("\n");
            }
            sb.append("  ]\n}");
            return sb.toString();
        }
    }

    // ---- DSN conversion ----------------------------------------------------

    static String dsnToJdbc(String dsn) throws Exception {
        URI uri = new URI(dsn);
        String user = "", password = "";
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            int colon = userInfo.indexOf(':');
            if (colon >= 0) {
                user     = userInfo.substring(0, colon);
                password = userInfo.substring(colon + 1);
            } else {
                user = userInfo;
            }
        }
        int    port = uri.getPort() > 0 ? uri.getPort() : 5432;
        String db   = uri.getPath();
        if (db.startsWith("/")) db = db.substring(1);

        return "jdbc:postgresql://" + uri.getHost() + ":" + port + "/" + db
             + "?user=" + user
             + "&password=" + password
             + "&ssl=false"
             + "&prepareThreshold=1"
             + "&loginTimeout=15"
             + "&socketTimeout=30";
    }

    // ---- Scenario ----------------------------------------------------------

    static Summary runPreparedFailover(String dsn, int iterations) {
        Summary s = new Summary(iterations);

        String jdbcUrl;
        try {
            jdbcUrl = dsnToJdbc(dsn);
        } catch (Exception e) {
            for (int i = 1; i <= iterations; i++) {
                s.recordOutcome(new IterationResult(i, "", "", "", "",
                    false, "error", Instant.now(), "invalid DSN: " + e.getMessage()));
            }
            return s;
        }

        Instant start = Instant.now();
        String testID = String.format("driver_jdbc_%s%06d",
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_")
                             .withZone(ZoneOffset.UTC)
                             .format(start),
            start.getNano() / 1000 % 1_000_000);
        String clientID = "jdbc";

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.setAutoCommit(true);

            try (Statement st = conn.createStatement()) {
                st.execute(
                    "CREATE TABLE IF NOT EXISTS ktf_events (" +
                    "  test_id    text         NOT NULL," +
                    "  client_id  text         NOT NULL," +
                    "  seq        bigint       NOT NULL," +
                    "  payload    text         NOT NULL," +
                    "  created_at timestamptz  NOT NULL DEFAULT clock_timestamp()," +
                    "  PRIMARY KEY (test_id, client_id, seq)" +
                    ")"
                );
            }

            try (
                PreparedStatement writeStmt = conn.prepareStatement(
                    "INSERT INTO ktf_events(test_id, client_id, seq, payload) " +
                    "VALUES (?, ?, ?, ?) " +
                    "RETURNING pg_current_wal_lsn()::text, txid_current()::text"
                );
                PreparedStatement readStmt = conn.prepareStatement(
                    "SELECT payload FROM ktf_events " +
                    "WHERE test_id = ? AND client_id = ? AND seq = ?"
                )
            ) {
                for (int i = 1; i <= iterations; i++) {
                    String  payload     = "ktf:" + testID + ":" + i;
                    Instant occurredAt  = Instant.now();
                    String  commitLSN   = "";
                    String  txID        = "";
                    boolean commitAck   = false;

                    try {
                        writeStmt.setString(1, testID);
                        writeStmt.setString(2, clientID);
                        writeStmt.setLong  (3, i);
                        writeStmt.setString(4, payload);
                        try (ResultSet rs = writeStmt.executeQuery()) {
                            if (rs.next()) {
                                commitLSN = rs.getString(1);
                                txID      = rs.getString(2);
                                commitAck = true;
                            }
                        }
                    } catch (SQLException e) {
                        s.recordOutcome(new IterationResult(i, payload, "",
                            "", "", false, "error", occurredAt, e.getMessage()));
                        continue;
                    }

                    String observed = null;
                    try {
                        readStmt.setString(1, testID);
                        readStmt.setString(2, clientID);
                        readStmt.setLong  (3, i);
                        try (ResultSet rs = readStmt.executeQuery()) {
                            if (rs.next()) observed = rs.getString(1);
                        }
                    } catch (SQLException e) {
                        String outcome = commitAck ? "error" : "ambiguous";
                        s.recordOutcome(new IterationResult(i, payload, "",
                            commitLSN, txID, commitAck, outcome, occurredAt, e.getMessage()));
                        continue;
                    }

                    if (observed == null || observed.isEmpty()
                        || !observed.trim().equals(payload)) {
                        s.recordOutcome(new IterationResult(i, payload,
                            observed == null ? "" : observed,
                            commitLSN, txID, commitAck, "stale_read", occurredAt, null));
                    } else {
                        s.recordOutcome(new IterationResult(i, payload, observed,
                            commitLSN, txID, commitAck, "pass", occurredAt, null));
                    }
                }
            }

        } catch (SQLException e) {
            int done = s.results.size();
            for (int i = done + 1; i <= iterations; i++) {
                s.recordOutcome(new IterationResult(i, "", "", "", "",
                    false, "error", Instant.now(), "connection: " + e.getMessage()));
            }
        }

        return s;
    }

    // ---- Entry point -------------------------------------------------------

    public static void main(String[] args) {
        String  dsn        = System.getenv("KEEL_DSN");
        String  scenario   = "prepared-failover";
        int     iterations = 100;
        boolean asJSON     = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--dsn"        -> dsn        = args[++i];
                case "--scenario"   -> scenario   = args[++i];
                case "--iterations" -> iterations = Integer.parseInt(args[++i]);
                case "--json"       -> asJSON      = true;
                default -> { /* ignore unknown flags */ }
            }
        }

        if (dsn == null || dsn.isEmpty()) {
            System.err.println("missing --dsn or KEEL_DSN");
            System.exit(2);
        }
        if (!scenario.equals("prepared-failover")) {
            System.err.println("unsupported scenario: " + scenario);
            System.exit(2);
        }

        Summary summary = runPreparedFailover(dsn, iterations);

        if (asJSON) {
            System.out.println(summary.toJson());
        } else {
            System.out.printf(
                "total=%d passed=%d stale=%d ambiguous=%d errors=%d first_error_seq=%d%n",
                summary.total, summary.passed, summary.staleReads,
                summary.ambiguous, summary.errors, summary.firstErrorSeq);
        }

        boolean clean = summary.errors == 0
                     && summary.ambiguous == 0
                     && summary.staleReads == 0
                     && summary.passed == summary.total;
        System.exit(clean ? 0 : 1);
    }
}
