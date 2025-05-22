import pandas as pd
from pathlib import Path

def count_units(df, unit_column, target_units, case_sensitive=False):
    """
    Counts the number of occurrences of specified units in a DataFrame column.
    """
    if unit_column not in df.columns:
        print(f"Error: Unit column '{unit_column}' not found in the DataFrame.")
        return 0

    if isinstance(target_units, str):
        target_units = [target_units]  # Ensure target_units is a list

    if case_sensitive:
        unit_counts = df[unit_column].isin(target_units).sum()
    else:
        unit_counts = df[unit_column].str.lower().isin([unit.lower() for unit in target_units]).sum()
    return unit_counts


def get_unique_units(df, unit_column, unique_units_set):
    """
    Gets all unique units from a specified column in a DataFrame, case-insensitively.

    Args:
        df (pd.DataFrame): The input DataFrame.
        unit_column (str): The name of the column containing the units.
        unique_units_set (set): A set to store the unique units.  This is modified in place.

    Returns:
        None: The function modifies the unique_units_set directly.
    """
    if unit_column not in df.columns:
        print(f"Error: Unit column '{unit_column}' not found in the DataFrame.")

    else:
        unique_units_chunk = df[unit_column].unique()
        unique_units_lower = [unit.lower() for unit in unique_units_chunk]
        unique_units_set.update(unique_units_lower)



def main():
    """
    Main function to process the safecast data and count units.
    """
    chunk_size = 1000000
    cpm_count = 0
    usv_count = 0
    usv_per_hr_count = 0
    column_name = 'Unit'
    unique_units_set = set() # Changed to a set
    i = 1

    script_dir = Path(__file__).parent
    data_dir = script_dir / "measurements-out.csv"

    for chunk in pd.read_csv(
        data_dir,
        chunksize=chunk_size,
        dtype={column_name: str},  # Specify dtype for 'Unit' column
        low_memory=False,
    ):
        # Use the count_units function for each unit
        cpm_count += count_units(chunk, column_name, ['cpm', ' cpm'], case_sensitive=False)
        usv_count += count_units(chunk, column_name, ['usv', 'microsievert'], case_sensitive=False)
        usv_per_hr_count += count_units(chunk, column_name, ['usv/h', 'usv/hr'], case_sensitive=False)
        # Get unique units
        # get_unique_units(chunk, column_name, unique_units_set)
        print("Iteration: ", i)
        i += 1
        print(chunk.head(5))

        # check only first chunk
        if cpm_count >= chunk_size:
            break

    # print(f"cpm: {cpm_count}")
    # print(f"uSv: {usv_count}")
    # print(f"uSv/h: {usv_per_hr_count}")

    # Print unique units
    print(f"Unique units (lowercase): {sorted(list(unique_units_set))}") #convert set to list before printing and sort it.



if __name__ == "__main__":
    main()
