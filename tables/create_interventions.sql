CREATE TABLE intervention_data (
    postal INT NOT NULL, 
    block VARCHAR(10),
    premise_type VARCHAR(50),
    ehi_sector_id VARCHAR(10),  -- Environmental Health Institute sector ID
    sector_id VARCHAR(10),  -- Identifier for the sector
    total_dwelling INT, 
    release_date DATE,
    PRIMARY KEY (postal)  -- Primary key is the postal code
);
COMMENT ON COLUMN intervention_data.postal IS 'The postal code of the location (6 digits)';
COMMENT ON COLUMN intervention_data.block IS 'The block number associated with the postal code';
COMMENT ON COLUMN intervention_data.premise_type IS 'Type of premise (e.g., HDB_RESIDENTIAL, LANDED_RESIDENTIAL)';
COMMENT ON COLUMN intervention_data.ehi_sector_id IS 'Environmental Health Institute sector ID';
COMMENT ON COLUMN intervention_data.sector_id IS 'Identifier for the sector';
COMMENT ON COLUMN intervention_data.release_date IS 'Wolbachia release date';
COMMENT ON COLUMN intervention_data.release_type IS 'Wolbachia release strategy'
COMMENT ON COLUMN intervention_data.total_dwelling IS 'Total number of dwellings in the area of intervention';
