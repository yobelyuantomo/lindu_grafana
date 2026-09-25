CREATE TABLE IF NOT EXISTS sensor_telemetry (
    time         TIMESTAMP WITH TIME ZONE NOT NULL,
    node_id      VARCHAR(50) NOT NULL,
    pga          DOUBLE PRECISION,
    rms          DOUBLE PRECISION,
    accel_x      DOUBLE PRECISION,
    accel_y      DOUBLE PRECISION,
    accel_z      DOUBLE PRECISION,
    temperature  DOUBLE PRECISION,
    pressure     DOUBLE PRECISION,
    humidity     DOUBLE PRECISION,
    latency_ms   DOUBLE PRECISION,
    valve_status VARCHAR(20),
    gas_raw      INTEGER,
    gas_alert    BOOLEAN,
    freq_hz      INTEGER,
    sensor_ts    DOUBLE PRECISION
);

-- init.sql hanya jalan sekali saat volume postgres pertama kali dibuat, jadi
-- kolom baru di atas ditambahkan lagi di sini untuk deployment yang sudah
-- ada sebelumnya (volume lama tanpa kolom gas).
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS gas_raw INTEGER;
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS gas_alert BOOLEAN;

-- freq_hz dan sensor_ts dipakai sebagai fitur dataset ML (Assignment 3).
-- freq_hz sebelumnya hanya tersimpan di tb_sensor_telemetry, dan itu pun hanya
-- untuk baris yang sudah lolos filter rule-based — sehingga kelas negatif
-- (getaran non-gempa) tidak pernah punya nilai frekuensi sama sekali.
-- sensor_ts menyimpan epoch asli dari node; kolom `time` adalah waktu terima
-- di server, yang tidak cukup presisi untuk menyusun jendela sinyal 1-2 detik.
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS freq_hz INTEGER;
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS sensor_ts DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS sensor_status (
    time            TIMESTAMP WITH TIME ZONE NOT NULL,
    node_id         VARCHAR(50) NOT NULL,
    status          VARCHAR(20),
    pose            VARCHAR(20),
    tilt_angle      DOUBLE PRECISION,
    latency_ms      DOUBLE PRECISION,
    sensor_ok       BOOLEAN,
    fw_version      VARCHAR(50),
    ota_status      VARCHAR(50),
    motion_detected BOOLEAN
);

-- Kolom PIR ditambahkan belakangan; ALTER di sini agar deployment lama (volume
-- postgres yang sudah ada) ikut dapat kolomnya juga, sama seperti pola gas_raw/gas_alert di atas.
ALTER TABLE sensor_status ADD COLUMN IF NOT EXISTS motion_detected BOOLEAN;

-- Lokasi node terakhir yang diketahui (dipakai panel peta "Live Seismic Map").
-- Di-upsert oleh ingester setiap kali menerima pesan status dari node.
CREATE TABLE IF NOT EXISTS tb_nodes (
    node_id    VARCHAR(50) PRIMARY KEY,
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION,
    updated_at TIMESTAMP WITH TIME ZONE
);
