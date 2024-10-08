CREATE TABLE IF NOT EXISTS national_analysis.elnino34 (
    date DATE PRIMARY KEY,
    sst NUMERIC,
    ssta NUMERIC
);

COMMENT ON TABLE national_analysis.elnino34 IS 'Nino3.4 refers to a specific region in the equatorial Pacific Ocean that is closely monitored for sea surface temperature (SST) anomalies, which are important indicators for El Niño and La Niña events';

COMMENT ON COLUMN national_analysis.elnino34.date IS 'date of the record';
COMMENT ON COLUMN national_analysis.elnino34.sst IS 'SST (Sea Surface Temperature) value of Nino 34';
COMMENT ON COLUMN national_analysis.elnino34.ssta IS 'SSTA (Sea Surface Temperature Anomaly) value of Nino 34';0