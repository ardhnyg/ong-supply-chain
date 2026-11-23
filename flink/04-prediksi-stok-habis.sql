-- 04. Prediksi stok habis per SPBU per produk.
-- runout_minutes = stok saat ini / rata-rata penjualan per menit dalam 5 menit terakhir.
-- Status KRITIS bila stok diperkirakan habis dalam kurang dari 10 menit (ambang demo,
-- karena skala waktu dipadatkan; di lapangan misalnya 6 jam).

-- CELL 1. Statement yang jalan terus.
CREATE TABLE spbu_runout_forecast AS
SELECT
  spbu_id,
  product,
  event_time,
  stock_liters,
  sales_5m_liters,
  CASE WHEN sales_5m_liters > 0
       THEN ROUND(CAST(GREATEST(stock_liters, 0) AS DOUBLE) / (sales_5m_liters / 5.0), 1)
  END AS runout_minutes,
  CASE
    WHEN stock_liters <= 0 THEN 'KOSONG'
    WHEN sales_5m_liters > 0
     AND CAST(stock_liters AS DOUBLE) / (sales_5m_liters / 5.0) < 10 THEN 'KRITIS'
    ELSE 'AMAN'
  END AS status
FROM (
  SELECT
    spbu_id,
    product,
    event_time,
    stock_liters,
    SUM(sales_liters) OVER (
      PARTITION BY spbu_id, product
      ORDER BY $rowtime
      RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) AS sales_5m_liters
  FROM spbu_stock_running
);

-- CELL 2. Satu alert per tangki per menit selama statusnya bukan AMAN.
-- Ini yang dikirim ke penjadwalan mobil tangki / dashboard command center.
CREATE TABLE supply_alerts AS
SELECT
  spbu_id,
  product,
  window_end                 AS alert_time,
  MIN(stock_liters)          AS stock_liters,
  MIN(runout_minutes)        AS runout_minutes,
  CASE WHEN MIN(stock_liters) <= 0 THEN 'KOSONG' ELSE 'KRITIS' END AS status
FROM TABLE(TUMBLE(TABLE spbu_runout_forecast, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE))
WHERE status <> 'AMAN'
GROUP BY spbu_id, product, window_start, window_end;

-- CELL 3. Dashboard sederhana di workspace: posisi stok terakhir tiap tangki.
SELECT spbu_id, product, stock_liters, runout_minutes, status, event_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY spbu_id, product ORDER BY $rowtime DESC) AS rn
  FROM spbu_runout_forecast
)
WHERE rn = 1;
