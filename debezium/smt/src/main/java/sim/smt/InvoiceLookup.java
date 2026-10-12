package sim.smt;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Approach 4: enriches each Debezium change event with a synchronous JDBC lookup of the
 * invoice it belongs to, before the event is written to Kafka.
 *
 * The lookup sees invoice_batch as it is when the event passes through Kafka Connect, not as
 * it was when the change committed - which is exactly the race this approach is known for.
 * The result is added to the envelope as an "enrichment" struct (null fields on a miss).
 */
public class InvoiceLookup<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define("jdbc.url", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "JDBC URL of the lookup database")
        .define("jdbc.user", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Lookup database user")
        .define("jdbc.password", ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Lookup database password")
        .define("lookup.field", ConfigDef.Type.STRING, "batch_id", ConfigDef.Importance.LOW, "Row field holding the invoice_batch id");

    private static final String LOOKUP_SQL = "SELECT invoice_number, status FROM dbo.invoice_batch WHERE id = ?";
    private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    private static final Schema ENRICHMENT_SCHEMA = SchemaBuilder.struct()
        .name("sim.smt.Enrichment")
        .optional()
        .field("invoice_number", Schema.OPTIONAL_STRING_SCHEMA)
        .field("invoice_status", Schema.OPTIONAL_STRING_SCHEMA)
        .field("lookup_ts_ms", Schema.INT64_SCHEMA)
        .field("lookup_us", Schema.INT64_SCHEMA)
        .build();

    private String url;
    private String user;
    private String password;
    private String lookupField;
    private Connection connection;
    private PreparedStatement statement;
    private final Map<Schema, Schema> schemaCache = new HashMap<>();

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig cfg = new AbstractConfig(CONFIG_DEF, props);
        url = cfg.getString("jdbc.url");
        user = cfg.getString("jdbc.user");
        password = cfg.getPassword("jdbc.password").value();
        lookupField = cfg.getString("lookup.field");
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct) || record.valueSchema() == null) {
            return record; // tombstones and non-envelope records pass through untouched
        }
        Schema valueSchema = record.valueSchema();
        if (valueSchema.field("op") == null || valueSchema.field("after") == null) {
            return record;
        }
        Struct value = (Struct) record.value();
        Struct row = value.getStruct("after");
        if (row == null && valueSchema.field("before") != null) {
            row = value.getStruct("before");
        }
        if (row == null || row.schema().field(lookupField) == null) {
            return record;
        }

        long start = System.nanoTime();
        String[] invoice = lookup(row.get(lookupField));
        long micros = (System.nanoTime() - start) / 1000;

        Schema enrichedSchema = schemaCache.computeIfAbsent(valueSchema, InvoiceLookup::withEnrichment);
        Struct enriched = new Struct(enrichedSchema);
        for (Field f : valueSchema.fields()) {
            enriched.put(f.name(), value.get(f));
        }
        enriched.put("enrichment", new Struct(ENRICHMENT_SCHEMA)
            .put("invoice_number", invoice[0])
            .put("invoice_status", invoice[1])
            .put("lookup_ts_ms", System.currentTimeMillis())
            .put("lookup_us", micros));

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
            enrichedSchema, enriched, record.timestamp(), record.headers());
    }

    private static Schema withEnrichment(Schema envelope) {
        SchemaBuilder builder = SchemaBuilder.struct().name(envelope.name());
        if (envelope.version() != null) builder.version(envelope.version());
        if (envelope.doc() != null) builder.doc(envelope.doc());
        if (envelope.isOptional()) builder.optional();
        for (Field f : envelope.fields()) {
            builder.field(f.name(), f.schema());
        }
        builder.field("enrichment", ENRICHMENT_SCHEMA);
        return builder.build();
    }

    private String[] lookup(Object key) {
        SQLException last = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                ensureConnection();
                statement.setObject(1, key);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        return new String[] { rs.getString(1), rs.getString(2) };
                    }
                    return new String[] { null, null };
                }
            } catch (SQLException e) {
                last = e;
                closeQuietly();
                try {
                    Thread.sleep(200L * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw new ConnectException("Invoice lookup failed after retries", last);
    }

    private void ensureConnection() throws SQLException {
        if (connection != null) return;
        try {
            Driver driver = (Driver) Class.forName(DRIVER_CLASS, true, getClass().getClassLoader())
                .getDeclaredConstructor().newInstance();
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            connection = driver.connect(url, props);
            if (connection == null) throw new SQLException("Driver rejected URL " + url);
            statement = connection.prepareStatement(LOOKUP_SQL);
        } catch (ReflectiveOperationException e) {
            throw new SQLException("SQL Server JDBC driver not available to the SMT", e);
        }
    }

    private void closeQuietly() {
        try { if (statement != null) statement.close(); } catch (SQLException ignored) { }
        try { if (connection != null) connection.close(); } catch (SQLException ignored) { }
        statement = null;
        connection = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        closeQuietly();
    }
}
