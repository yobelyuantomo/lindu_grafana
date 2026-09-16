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
    gas_alert    BOOLEAN
);

-- init.sql hanya jalan sekali saat volume postgres pertama kali dibuat, jadi
-- kolom baru di atas ditambahkan lagi di sini untuk deployment yang sudah
-- ada sebelumnya (volume lama tanpa kolom gas).
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS gas_raw INTEGER;
ALTER TABLE sensor_telemetry ADD COLUMN IF NOT EXISTS gas_alert BOOLEAN;

CREATE TABLE IF NOT EXISTS sensor_status (
    time        TIMESTAMP WITH TIME ZONE NOT NULL,
    node_id     VARCHAR(50) NOT NULL,
    status      VARCHAR(20),
    pose        VARCHAR(20),
    tilt_angle  DOUBLE PRECISION,
    latency_ms  DOUBLE PRECISION,
    sensor_ok   BOOLEAN,
    fw_version  VARCHAR(50),
    ota_status  VARCHAR(50)
);

-- Lokasi node terakhir yang diketahui (dipakai panel peta "Live Seismic Map").
-- Di-upsert oleh ingester setiap kali menerima pesan status dari node.
CREATE TABLE IF NOT EXISTS tb_nodes (
    node_id    VARCHAR(50) PRIMARY KEY,
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION,
    updated_at TIMESTAMP WITH TIME ZONE
);
