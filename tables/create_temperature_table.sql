CREATE TABLE national_analysis.temperature (
    station_id VARCHAR(50) NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    dbt_max FLOAT,
    dbt_min FLOAT,
    dbt_mean FLOAT,
    rh_max FLOAT,
    rh_min FLOAT,
    rh_mean FLOAT,
    PRIMARY KEY (station_id, date)
);
-- Add comment to the table
COMMENT ON TABLE national_analysis.temperature IS 'Table storing temperature and humidity data for weather stations, partitioned by date.';

-- Add comments to each column
COMMENT ON COLUMN national_analysis.temperature.station_id IS 'Identifier for the weather station (string)';
COMMENT ON COLUMN national_analysis.temperature.date IS 'Date and time of the observation, with timezone information';
COMMENT ON COLUMN national_analysis.temperature.dbt_max IS 'Maximum Dry Bulb Temperature in degrees';
COMMENT ON COLUMN national_analysis.temperature.dbt_min IS 'Minimum Dry Bulb Temperature in degrees';
COMMENT ON COLUMN national_analysis.temperature.dbt_mean IS 'Mean Dry Bulb Temperature in degrees';
COMMENT ON COLUMN national_analysis.temperature.rh_max IS 'Maximum Relative Humidity (in percentage)';
COMMENT ON COLUMN national_analysis.temperature.rh_min IS 'Minimum Relative Humidity (in percentage)';
COMMENT ON COLUMN national_analysis.temperature.rh_mean IS 'Mean Relative Humidity (in percentage)';
