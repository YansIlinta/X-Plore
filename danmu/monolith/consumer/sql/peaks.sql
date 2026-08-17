-- 弹幕峰值打点 / 高能进度条：ClickHouse 物化视图与回填
--
-- consumer 启动时会 CREATE MATERIALIZED VIEW IF NOT EXISTS（见 consumer/db.go）。
-- MV 只捕获创建后的新 INSERT；若要对历史数据建密度序列，执行下方回填：

-- 回填历史密度（幂等：SummingMergeTree 会合并同键）
INSERT INTO danmu_count_per_sec
SELECT
    room_id,
    toStartOfSecond(fromUnixTimestamp64Milli(server_ts)) AS ts_bucket,
    count() AS cnt
FROM danmu_history
GROUP BY room_id, ts_bucket;

-- 查询示例（server HistoryDB.QueryPeaks 优先走 MV，失败回退直接 GROUP BY）
-- SELECT toUnixTimestamp(ts_bucket) AS ts, sum(cnt) AS cnt
-- FROM danmu_count_per_sec
-- WHERE room_id = 'room-1'
-- GROUP BY ts ORDER BY ts;
