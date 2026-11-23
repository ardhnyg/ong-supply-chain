-- 01. Cek data dari Datagen. Di SQL workspace: satu statement per cell.
-- Tabel spbu_sales, terminal_loading, dan spbu_receipt otomatis ada begitu topic
-- dan schema Avro terdaftar. $rowtime = timestamp record Kafka, dipakai sebagai event time.

-- CELL 1. Penjualan POS
SELECT spbu_id, product, nozzle, liters, scenario, $rowtime FROM spbu_sales;

-- CELL 2. Pemuatan di gantry terminal dan penerimaan di ATG SPBU, per nomor DO
SELECT do_number, terminal_id, truck_id, spbu_id, product, loaded_liters, $rowtime FROM terminal_loading;

-- CELL 3.
SELECT do_number, received_liters, $rowtime FROM spbu_receipt;

-- CELL 4. Penjualan per SPBU per produk per menit
SELECT spbu_id, product, window_start, COUNT(*) AS transaksi, SUM(liters) AS liter
FROM TABLE(TUMBLE(TABLE spbu_sales, DESCRIPTOR($rowtime), INTERVAL '1' MINUTE))
GROUP BY spbu_id, product, window_start, window_end;
