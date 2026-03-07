-- Partition metadata table for spream.
-- Adjust table name and index names according to your naming conventions.

CREATE TABLE IF NOT EXISTS PartitionMetadata (
  PartitionToken STRING(MAX) NOT NULL,
  ParentTokens ARRAY<STRING(MAX)> NOT NULL,
  StartTimestamp TIMESTAMP NOT NULL,
  EndTimestamp TIMESTAMP NOT NULL,
  HeartbeatMillis INT64 NOT NULL,
  State STRING(MAX) NOT NULL,
  Watermark TIMESTAMP NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  ScheduledAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  RunningAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  FinishedAt TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (PartitionToken),
  ROW DELETION POLICY (OLDER_THAN(FinishedAt, INTERVAL 1 DAY));

-- Recommended indexes for query optimization.

-- For GetUnfinishedMinWatermarkPartition and GetInterruptedPartitions
CREATE INDEX PartitionMetadataByStateWatermark ON PartitionMetadata (State, Watermark);

-- For GetSchedulablePartitions
CREATE INDEX PartitionMetadataByStateStartTimestamp ON PartitionMetadata (State, StartTimestamp);
