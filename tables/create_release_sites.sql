CREATE SCHEMA IF NOT EXISTS national_analysis;
CREATE TABLE IF NOT EXISTS national_analysis.site_release (
    postal INT NOT NULL,
    sector_id VARCHAR(10),
    premise_type VARCHAR(50),
    release_date DATE,
    total_dwelling INT,
    PRIMARY KEY (postal)
);

COMMENT ON COLUMN national_analysis.site_release.postal IS 'The postal code of the location (6 digits)';
COMMENT ON COLUMN national_analysis.site_release.sector_id IS 'Sector ID';
COMMENT ON COLUMN national_analysis.site_release.premise_type IS 'Type of premise (e.g., RESIDENTIAL_HDB, RESIDENTIAL_LANDED)';
COMMENT ON COLUMN national_analysis.site_release.release_date IS 'Wolbachia release date';
COMMENT ON COLUMN national_analysis.site_release.total_dwelling IS 'Total number of dwellings in the area of intervention';
