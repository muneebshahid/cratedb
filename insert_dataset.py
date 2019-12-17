"""Creates tables and inserts the dataset.
"""

import os
from typing import List

import pandas as pd
from crate import client
from crate.client.cursor import Cursor
from tqdm import tqdm


from tables import (
    CALENDAR,
    CALENDAR_DATES,
    CREATE_TABLE_QUERIES,
    SERVICE_ALERTS,
    SHAPES,
    STOPS,
    STOP_TIMES,
    TABLES,
)

DATASET_DIR = "GTFS"


def drop_tables(Cursor: Cursor) -> None:
    """Drops all created tables.

    Args:
        Cursor (Cursor): Crate db cursor

    """
    for table in TABLES:
        cursor.execute(f"Drop table IF EXISTS {table}")


def create_tables(cursor: Cursor) -> None:
    """Creates tables if needed.

    Args:
        cursor (Cursor): Crate db cursor.

    Returns:
        None

    """
    for create_query in CREATE_TABLE_QUERIES.values():
        cursor.execute(create_query)


def load_df(file_name: str) -> pd.DataFrame:
    """Loads the file in a dataframe.

    Args:
        file_name (str): File to be loaded

    Returns:
        pd.DataFrame: Loaded dataframe.

    """
    full_file_path = os.path.join(DATASET_DIR, file_name + ".txt")
    if file_name == SERVICE_ALERTS:
        # Load the file and convert it to a pandas ready dict.
        with open(full_file_path, "r") as f:
            # Load lines with ':' in them.
            lines = [
                line.replace("\n", "").replace(" ", "").replace('"', "").split(":")
                for line in f.readlines()
                if ":" in line
            ]
            lines = {line[0]: [line[1]] for line in lines}
            df = pd.DataFrame.from_dict(lines)
    else:
        # Replace nans with None
        df = pd.read_csv(os.path.join(DATASET_DIR, file_name + ".txt"))
    df.replace({pd.np.nan: None}, inplace=True)
    return df


def combine_lon_lat(
    df: pd.DataFrame, lon_column: str, lat_column: str, location_column: str
) -> None:
    """Concatenates and replaces lon_column lat_column columns in to 1 location_column.
    location_column has the same index as the original lon_column. Changes are done
    inplace if possible.

    Args:
        df (pd.DataFrame): Dataframe with lat and lon columns.
        lon_column (str): Longitude.
        lat_column (str): Latitude.
        location_column (str): Combined array of [lon_column, lat_column]

    Returns:
        None

    """
    # Combine lat lon column in to one location column
    df[lon_column] = df[[lon_column, lat_column]].values.tolist()
    df.drop(columns=[lat_column], inplace=True)
    df.rename(columns={lon_column: location_column}, inplace=True)


def to_datetime(df: pd.DataFrame, column: str) -> None:
    """Converts datetime column to pandas datetime.

    Args:
        df (pd.DataFrame): Dataframe to be parsed.
        column (str): Column to be converted.

    Returns:
        None

    """
    df[column] = pd.to_datetime(df[column], format="%Y%m%d")


def load_file(file_name: str) -> pd.DataFrame:
    """Loads the given file and preprocess it afterwards returns the preprocessed
    dataframe.

    Args:
        file_name (str): File to be loaded

    Returns:
        pd.DataFrame: Dataframe with each row representing one line.

    """
    df = load_df(file_name)

    # Preprocess date columns
    if file_name == CALENDAR_DATES:
        to_datetime(df, "date")
    elif file_name == CALENDAR:
        to_datetime(df, "start_date")
        to_datetime(df, "end_date")

    # Combine lat lon column in to one location column
    elif file_name == SHAPES:
        combine_lon_lat(df, "shape_pt_lon", "shape_pt_lat", "shape_pt_location")
    elif file_name == STOPS:
        combine_lon_lat(df, "stop_lon", "stop_lat", "stop_location")
    return df


def insert_dataset(cursor: Cursor) -> None:
    """Loops over all files and inserts the whole dataset.

    Args:
        cursor (Cursor): [description]
    Returns:
        None

    """
    for table in TABLES:
        print(f"Ingesting {table}")
        df = load_file(table)

        # Ugly hack to replace quotes and get correct number of argument tuple.
        arg_tuple = str(tuple(["?"]) * len(df.columns)).replace("'", "")

        # I couldn't find a bulk size argument for executemany so just made my own
        # wrapper around it.
        CHUNK = 50000
        for i in tqdm(range(0, len(df), CHUNK)):

            # Get current chunk view.
            df_view = df[i : i + CHUNK].values.tolist()

            if len(df_view) > 0:
                # Actually insert the data.
                cursor.executemany(f"INSERT INTO {table} VALUES {arg_tuple}", df_view)
        print(f"Done Ingesting {table} \n")


if __name__ == "__main__":
    try:
        if not os.path.isdir(DATASET_DIR):
            raise Exception(f"Dataset dir {DATASET_DIR} not found.")
        # Create the connection and the cursor.
        connection = client.connect("http://localhost:4200/", username="crate")
        cursor = connection.cursor()
        drop_tables(cursor)
        create_tables(cursor)
        insert_dataset(cursor)
    except Exception as e:
        print(f"Program did not execute properly because of exception {e}.")
