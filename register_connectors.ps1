$ErrorActionPreference = 'Stop'

$debeziumConfig = @{
    name = "billing_record_connector"
    config = @{
        "connector.class" = "io.debezium.connector.sqlserver.SqlServerConnector"
        "database.hostname" = "sqlserver"
        "database.port" = "1433"
        "database.user" = "sa"
        "database.password" = "Password123!"
        "database.names" = "sim_db"
        "topic.prefix" = "sim"
        "table.include.list" = "dbo.billing_record,dbo.invoice_batch,dbo.cdc_events_shadow,dbo.outbox_events"
        "database.encrypt" = "false"
        "schema.history.internal.kafka.bootstrap.servers" = "kafka:29092"
        "schema.history.internal.kafka.topic" = "schema-changes.billing"
    }
}

$jsonConfig = $debeziumConfig | ConvertTo-Json -Depth 10

Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -Body $jsonConfig -ContentType "application/json"
Write-Host "Debezium connector registered!"
