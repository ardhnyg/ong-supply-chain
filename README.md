# Demo Rantai Pasok sampai SPBU di Confluent Cloud: Datagen + Flink

Skenario 07 dari deck dan simulasi Pertamina Digital Hub. Semua langkah lewat Confluent Cloud Console.

Empat connector Datagen mensimulasikan transaksi POS SPBU, pemuatan mobil tangki di gantry terminal, dan penerimaan BBM yang tercatat ATG. Flink menghitung tiga hal:
1. **Rekonsiliasi per DO**: volume muat di terminal dibandingkan dengan volume diterima di SPBU.
2. **Stok buku per tangki**: stok awal + penerimaan − penjualan, diperbarui setiap ada transaksi.
3. **Prediksi stok habis**: stok dibagi rata-rata penjualan 5 menit terakhir, lalu alert bila hampir habis.

```
Datagen penjualan normal ──┐
Datagen penjualan lonjakan ─┴─> spbu_sales ───────────────┐
Datagen pemuatan terminal ────> terminal_loading ─┐        ├─> spbu_stock_running ─> spbu_runout_forecast ─> supply_alerts
Datagen penerimaan ATG ───────> spbu_receipt ─────┴─> do_reconciliation ─┘
                                                          └─> losses_alerts
```

## Isi folder

| File | Fungsi |
|---|---|
| `datagen/paste-schema-spbu-sales-normal.txt` | 4 SPBU, Pertalite dan Pertamax, 5–50 L per transaksi |
| `datagen/paste-schema-spbu-sales-lonjakan.txt` | Hanya SPBU-JKT-01 Pertalite, 20–60 L per transaksi |
| `datagen/paste-schema-terminal-loading.txt` | Satu record per DO: nomor DO berurutan, SPBU tujuan, produk, muat 8.000 L |
| `datagen/paste-schema-spbu-receipt.txt` | Satu record per DO: nomor DO berurutan, volume diterima ATG |
| `datagen/*.avsc` | Versi rapi dari schema yang sama |
| `flink/01-cek-data.sql` | Cek data masuk |
| `flink/02-rekonsiliasi-do.sql` | `do_reconciliation` dan `losses_alerts` |
| `flink/03-stok-berjalan.sql` | `spbu_stock_running` |
| `flink/04-prediksi-stok-habis.sql` | `spbu_runout_forecast`, `supply_alerts`, dan query posisi stok terakhir |

Semua ID SPBU, terminal, mobil tangki, dan angka volume adalah contoh.

## Cara kerja data simulasi

- **Nomor DO dicocokkan dengan `iteration`.** Connector pemuatan dan connector penerimaan sama-sama menghasilkan nomor DO berurutan mulai dari 1. Penerimaan untuk DO yang sama datang belakangan, lalu Flink menggabungkannya lewat interval join.
- **Volume diterima** dipilih acak dari 7.995, 7.990, 7.980, 7.970, 7.960, dan 7.600 liter. Lima nilai pertama selisihnya 0,06–0,5% dari 8.000 L (status OK). Nilai 7.600 selisih 5% dan masuk `losses_alerts`. Rata-rata satu dari enam DO menjadi losses.
- **Stok tidak dibaca dari ATG.** Datagen membangkitkan setiap record secara acak dan independen, jadi tidak bisa membuat level tangki yang turun perlahan. Karena itu stok dihitung sebagai stok buku: stok awal 50.000 L ditambah penerimaan, dikurangi penjualan. Di lapangan, stok buku ini divalidasi dengan pembacaan ATG.
- **Skala waktu dipadatkan.** Satu tangki Pertalite di demo menjual sekitar 18 liter per detik, jauh di atas SPBU sebenarnya. Tujuannya supaya perubahan stok terlihat dalam hitungan menit. Ambang KRITIS di demo adalah stok habis dalam kurang dari 10 menit.

Dengan rate di bawah, tangki lain tetap AMAN (perkiraan habis di atas 19 menit) selama kondisi normal. Saat connector lonjakan jalan, SPBU-JKT-01 Pertalite menjadi KRITIS sekitar 2–3 menit kemudian. Angka ini dari simulasi offline dengan parameter yang sama, bukan dari run di Confluent Cloud.

## Prasyarat di Confluent Cloud

- Environment dengan Stream Governance (Schema Registry) aktif.
- Satu Kafka cluster. Basic sudah cukup.
- Flink compute pool di cloud provider dan region yang sama dengan cluster.
- API key Kafka atau service account untuk connector.

## Persiapan (sekitar 15 menit sebelum demo)

### 1. Buat topic
Cluster → **Topics** → **Add topic**:

| Topic | Partisi |
|---|---|
| `spbu_sales` | 4 |
| `terminal_loading` | 1 |
| `spbu_receipt` | 1 |

### 2. Buat connector Datagen
Cluster → **Connectors** → **Add connector** → **Datagen Source**. Untuk semua connector:
- Output record value format: `AVRO`
- Schema: pilih opsi schema sendiri (Schema string), lalu paste isi file `paste-schema-*.txt`
- Tasks: `1`

| Nama connector | Topic | Schema | Max interval (ms) | Schema keyfield | Kapan dibuat |
|---|---|---|---|---|---|
| `supply-sales-normal` | `spbu_sales` | `paste-schema-spbu-sales-normal.txt` | `500` | `spbu_id` | Pertama |
| `supply-terminal-loading` | `terminal_loading` | `paste-schema-terminal-loading.txt` | `140000` | `spbu_id` | Setelah sales normal |
| `supply-spbu-receipt` | `spbu_receipt` | `paste-schema-spbu-receipt.txt` | `140000` | (kosong) | Sekitar 1 menit setelah loading |
| `supply-sales-lonjakan` | `spbu_sales` | `paste-schema-spbu-sales-lonjakan.txt` | `1000` | `spbu_id` | Terakhir, lalu langsung **Pause** |

Label di UI bisa sedikit berbeda. Nama property-nya: `schema.string`, `schema.keyfield`, `max.interval`, `output.data.format`.

Max interval 140.000 ms artinya jeda acak sampai 140 detik antar DO, rata-rata sekitar satu DO per 70 detik.

### 3. Jalankan Flink
Environment → **Flink** → **Open SQL workspace**. Pilih catalog = environment, database = cluster. Jalankan per cell, berurutan:
1. `01-cek-data.sql`: pastikan ketiga topic terisi. `do_number` di `terminal_loading` dan `spbu_receipt` harus naik dari 1.
2. `02-rekonsiliasi-do.sql`: `EXPLAIN`, lalu `do_reconciliation`, lalu `losses_alerts`.
3. `03-stok-berjalan.sql`: `EXPLAIN`, lalu `spbu_stock_running`. Tabel ini membaca `do_reconciliation`, jadi file 02 harus jalan lebih dulu.
4. `04-prediksi-stok-habis.sql`: `spbu_runout_forecast`, lalu `supply_alerts`.

Statement `CREATE TABLE ... AS` berjalan terus dan terlihat di **Flink → Statements**. Jumlahnya lima.

### 4. Biarkan berjalan 5–6 menit
Rata-rata penjualan memakai jendela 5 menit, jadi perkiraan stok habis baru stabil setelah itu.

## Alur demo

Siapkan tiga cell di workspace:
- Cell 3 di file 04 (posisi stok terakhir tiap tangki)
- `SELECT * FROM supply_alerts;`
- `SELECT * FROM losses_alerts;`

| Menit | Aksi di Console | Yang terlihat |
|---|---|---|
| 0 | Semua connector kecuali lonjakan sudah jalan | Delapan tangki berstatus AMAN. `do_reconciliation` terisi satu baris per DO yang sudah diterima. |
| 1 | **Resume** `supply-sales-lonjakan` | Penjualan SPBU-JKT-01 Pertalite naik, stoknya turun lebih cepat |
| 3–4 | — | `supply_alerts` mulai berisi SPBU-JKT-01 PERTALITE dengan status KRITIS, satu baris per menit |
| Kapan saja | — | DO dengan penerimaan 7.600 L muncul di `losses_alerts` dengan selisih 400 L (5%) |
| ±8 | **Pause** lonjakan | Kalau ada DO yang tiba di SPBU-JKT-01 Pertalite, stoknya naik dan status kembali AMAN |

Poin cerita ke customer: logika yang sama berjalan untuk semua SPBU sekaligus, dan alert muncul dari data yang mengalir, tanpa menunggu rekap. Pertamina sudah punya PIEDCC dan Auto Replenishment, jadi demo ini menunjukkan pola event-nya, bukan menggantikan sistem itu.

## Setelah demo

1. **Connectors**: hapus keempat connector `supply-*`.
2. **Flink → Statements**: stop kelima statement `CREATE TABLE ... AS`.
3. Topic hasil (`do_reconciliation`, `losses_alerts`, `spbu_stock_running`, `spbu_runout_forecast`, `supply_alerts`) dan topic sumber bisa dihapus dari menu Topics. Hati-hati: `DROP TABLE` di Flink juga menghapus topic beserta datanya.

## Yang perlu dicek saat pertama kali menjalankan

- **Restart connector mengulang nomor DO dari 1.** Kalau connector pemuatan atau penerimaan di-restart, nomor DO berulang dan join jadi kacau. Untuk mengulang demo dari bersih, hapus kedua connector, hapus dan buat ulang topic `terminal_loading` dan `spbu_receipt`, lalu buat ulang connectornya.
- **`iteration` pada tipe `long`.** Contoh resmi Datagen memakai `iteration` pada `int`. Kalau connector menolak schema, ganti `"type":"long"` menjadi `"type":"int"` untuk `do_number` di kedua schema.
- **Dua kolom waktu di interval join.** `loaded_at` dan `received_at` di-cast ke `TIMESTAMP(3)` supaya tabel hasil tidak punya dua kolom rowtime. Pastikan file 02 lolos `EXPLAIN`.
- **`spbu_stock_running` dan `spbu_runout_forecast`** memakai `$rowtime` dari topic hasil sebagai urutan waktu. Kalau hasilnya terlihat janggal, cek `DESCRIBE` dan nilai `$rowtime` di tabel tersebut.
- **Stok buku tidak dibatasi kapasitas tangki.** Kalau dibiarkan lama, stok tangki yang sering menerima DO bisa melewati 50.000 L. Untuk demo 15–20 menit, ini tidak terlihat.
- **ETA mobil tangki dari GPS** tidak dimasukkan supaya demonya tetap empat connector. Polanya sama: topic `mt_position`, lalu digabung dengan `spbu_runout_forecast`.
- **Biaya**: connector Datagen ditagih per jam task dan statement Flink memakai CFU.

## Referensi

- Datagen Source Connector for Confluent Cloud: https://docs.confluent.io/cloud/current/connectors/cc-datagen-source.html
- Contoh schema Datagen dengan `arg.properties` di dalam `type`: https://github.com/confluentinc/kafka-connect-datagen/tree/master/src/main/resources
