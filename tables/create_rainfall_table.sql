CREATE TABLE IF NOT EXISTS national_analysis.rainfall (
    station_id VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    rainfall_amt_total FLOAT,
    rainfall_duration_min FLOAT,
    PRIMARY KEY (station_id, date)
);

COMMENT ON TABLE national_analysis.rainfall_data IS 'This table stores the total rainfall and duration of rainfall for different stations on different dates.';

COMMENT ON COLUMN national_analysis.rainfall_data.station_id IS 'Unique identifier for each weather station.';
COMMENT ON COLUMN national_analysis.rainfall_data.date IS 'Date of the recorded rainfall event.';
COMMENT ON COLUMN national_analysis.rainfall_data.rainfall_amt_total IS 'Total rainfall amount measured in millimeters for the day.';
COMMENT ON COLUMN national_analysis.rainfall_data.rainfall_duration_min IS 'Total rainfall duration in minutes for the day.';