-- 03. Stok buku per SPBU per produk = stok awal + penerimaan (dari file 02) - penjualan.
-- Stok awal 50.000 L untuk semua tangki (contoh, skala demo dipadatkan).
-- Di lapangan, angka ini divalidasi dengan pembacaan ATG tangki pendam.

-- CELL 1. EXPLAIN dulu.
EXPLAIN
SELECT spbu_id, product, ts,
  50000 + SUM(delta_liters) OVER (
    PARTITION BY spbu_id, product ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stock_liters
FROM (
  SELECT spbu_id, product, $rowtime AS ts, -liters AS delta_liters, liters AS sales_liters FROM spbu_sales
  UNION ALL
  SELECT spbu_id, product, $rowtime AS ts, received_liters AS delta_liters, 0 AS sales_liters FROM do_reconciliation
);

-- CELL 2. Statement yang jalan terus. Satu baris per pergerakan stok.
CREATE TABLE spbu_stock_running AS
SELECT
  spbu_id,
  product,
  CAST(ts AS TIMESTAMP(3)) AS event_time,
  movement,
  delta_liters,
  sales_liters,
  50000 + SUM(delta_liters) OVER (
    PARTITION BY spbu_id, product
    ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS stock_liters
FROM (
  SELECT spbu_id, product, $rowtime AS ts, 'SALE' AS movement,
         -liters AS delta_liters, liters AS sales_liters
  FROM spbu_sales
  UNION ALL
  SELECT spbu_id, product, $rowtime AS ts, 'RECEIPT' AS movement,
         received_liters AS delta_liters, 0 AS sales_liters
  FROM do_reconciliation
);
