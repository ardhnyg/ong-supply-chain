-- 02. Rekonsiliasi per DO: volume muat di gantry terminal vs penerimaan ATG di SPBU.
-- Interval join pada do_number. Selisih di atas 0,5% (contoh toleransi) diberi status LOSSES.
-- Hasilnya juga dipakai file 03 sebagai penambah stok SPBU.

-- CELL 1. EXPLAIN dulu (gratis, tidak memakai CFU).
EXPLAIN
SELECT l.do_number, l.spbu_id, l.product, l.loaded_liters, r.received_liters
FROM terminal_loading l
JOIN spbu_receipt r
  ON l.do_number = r.do_number
 AND r.$rowtime BETWEEN l.$rowtime - INTERVAL '30' MINUTE AND l.$rowtime + INTERVAL '2' HOUR;

-- CELL 2. Statement yang jalan terus.
CREATE TABLE do_reconciliation AS
SELECT
  CONCAT('DO-', CAST(l.do_number AS STRING))                              AS do_id,
  l.do_number,
  l.terminal_id,
  l.truck_id,
  l.spbu_id,
  l.product,
  l.loaded_liters,
  r.received_liters,
  l.loaded_liters - r.received_liters                                      AS diff_liters,
  ROUND(CAST(l.loaded_liters - r.received_liters AS DOUBLE) * 100 / l.loaded_liters, 2) AS diff_pct,
  CASE
    WHEN CAST(l.loaded_liters - r.received_liters AS DOUBLE) > l.loaded_liters * 0.005 THEN 'LOSSES'
    ELSE 'OK'
  END                                                                      AS status,
  CAST(l.$rowtime AS TIMESTAMP(3))                                         AS loaded_at,
  CAST(r.$rowtime AS TIMESTAMP(3))                                         AS received_at
FROM terminal_loading l
JOIN spbu_receipt r
  ON l.do_number = r.do_number
 AND r.$rowtime BETWEEN l.$rowtime - INTERVAL '30' MINUTE AND l.$rowtime + INTERVAL '2' HOUR;

-- CELL 3. Topic alert khusus losses, untuk tim rekonsiliasi / SAP.
CREATE TABLE losses_alerts AS
SELECT do_id, terminal_id, truck_id, spbu_id, product, loaded_liters, received_liters, diff_liters, diff_pct
FROM do_reconciliation
WHERE status = 'LOSSES';
