cluster_name             = "my-kafka-cluster"
kafka_disk_size          =  10
kafka_disk_type_id       = "network-ssd"
kafka_resource_preset_id = "s2.micro"
kafka_version            = "3.9"

maintenance_window_type  = "WEEKLY"
maintenance_window_day = "MON"
maintenance_window_hour = 1

environment = "PRESTABLE"
brokers_count = 1
schema_registry = true
kafka_ui_enabled = true
rest_api_enabled = true

kraft_resource_preset_id = "s2.micro"
kraft_disk_size          = 10
kraft_disk_type_id       = "network-ssd"
assign_public_ip = true

kafka_replica_fetch_max_bytes = 2097152

# топик с 3 партициями и коэффициентом репликации 3.
topics = [
  {
    name               = "test-topic"
    partitions         = 3
    replication_factor = 3
    config = {
      cleanup_policy      = "CLEANUP_POLICY_COMPACT_AND_DELETE"
      compression_type    = "COMPRESSION_TYPE_LZ4"
      retention_ms        = 86400000         # 1 день 604800000    # 7 дней
      retention_bytes     = 2097152          # 2 MB 10737418240  # 10 GB
      max_message_bytes   = 2097152          # 2 MB 5242880      # 5 MB
      min_insync_replicas = 3
      segment_bytes       = 1073741824       # 1 GB
    }
  }
]
