"""Script for injesting release data."""

# TODO(Wesley): clean this up (not tested)

from os.path import join

from utils import read_excel_file

release_site_home = "/home/wesley/github/etheleon/national_analysis/data/release_site"

hdb_df = read_excel_file(join(release_site_home, "hdb.xlsx"))
hdb_df["PremiseType"].unique()


def insert_data_into_postgresql(df, connection):
    """Helper to add data to DB.

    Args:
        df (pd.DataFrame): The dataframe to be upserted
        connection: the connection to db
    """
    cursor = connection.cursor()

    insert_query = sql.SQL(
        """
        INSERT INTO national_analysis.site_release (
            postal, sector_id, premise_type, release_date, total_dwelling
        ) VALUES (%s, %s, %s, %s, %s)
    """
    )

    for index, row in df.iterrows():
        cursor.execute(
            insert_query,
            (
                int(row["postal"]),
                row["sector_id"],
                row["premise_type"],
                row["release_date"] if pd.notnull(row["release_date"]) else None,  # Handle NA dates
                int(row["total_dwelling"]) if pd.notnull(row["total_dwelling"]) else None,
            ),
        )

    connection.commit()
    print("Data inserted successfully into national_analysis.site_release!")


insert_data_into_postgresql(hdb_df_short, connection)
