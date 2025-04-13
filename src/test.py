import pandas as pd



def test_cutoff(data):
    # Convert visit_datetime to datetime type
    data['visit_datetime'] = pd.to_datetime(data['visit_datetime'])

    # Define the cutoff date
    cutoff_date = "2021-12-31"

    # Filter rows where visit_datetime is on or before the cutoff
    filtered = data[data['visit_datetime'] <= cutoff_date]

    # Print the filtered DataFrame
    print(filtered)

    print(data['lab_result_date'])

def check_doc_departments(data):
    dept_list = data['doctor_department'].unique()
    print(dept_list)
    # Extract the first letter of each word in each string
    # Extract and concatenate the first letter of each word in each string
    abbreviations = [
        ''.join(word[0] for word in string.split()).upper() for string in dept_list
    ]
    print(abbreviations)

def check_for_ids(data):
    list = data["patient_state"].unique()
    print(list)

def get_conventional_length(max_length: float) -> int:
    """
    Return the closest conventional VARCHAR length greater than or equal to max_length.
    
    Args:
        max_length: Maximum length of the column (float due to .max() output).
    
    Returns:
        int: Conventional VARCHAR length (e.g., 50, 100, 255, 500, 1000, etc.).
    """
    # Handle NaN or None case
    if pd.isna(max_length):
        return 50
    
    max_length = int(max_length)  # Convert float to int
    conventional_lengths = [50, 100, 255, 500, 1000, 2000, 4000, 8000]
    
    for length in conventional_lengths:
        if max_length <= length:
            return length
    
    # If exceeds all conventional lengths, round up to nearest 1000
    return ((max_length // 1000) + 1) * 1000

def check_lengths(data):
    # Identify string columns (object dtype typically indicates strings in Pandas)
    string_columns = data.select_dtypes(include=['object']).columns

    # Dictionary to store max lengths and conventional lengths for each string column
    max_lengths = {}
    varchar_lengths = {}

    # Check max length for each string column
    for column in string_columns:
        # Calculate the maximum length of non-null values in the column
        max_length = data[column].astype(str).str.len().max()
        max_lengths[column] = max_length if pd.notna(max_length) else 0
        # Get the conventional VARCHAR length
        varchar_lengths[column] = get_conventional_length(max_length)

    # Filter columns where max length exceeds 255 (optional, for reporting)
    exceeds_255 = {col: max_lengths[col] for col in max_lengths if max_lengths[col] > 255}

    # Print results
    print("Maximum lengths and suggested VARCHAR lengths for all string columns:")
    for col in string_columns:
        print(f"{col}: Max length = {max_lengths[col]}, Suggested VARCHAR({varchar_lengths[col]})")

    print("\nColumns with text length > 255:")
    if exceeds_255:
        for col, length in exceeds_255.items():
            print(f"{col}: Max length = {length} characters, Suggested VARCHAR({varchar_lengths[col]})")
    else:
        print("No string columns exceed 255 characters.")

    # Optional: Show sample rows for columns exceeding 255
    for col in exceeds_255.keys():
        print(f"\nSample rows for {col} with length > 255:")
        long_rows = data[data[col].astype(str).str.len() > 255][[col]]
        print(long_rows.head())

def get_conventional_length(max_length: float) -> int:
    """
    Return the closest conventional VARCHAR length greater than or equal to max_length.
    
    Args:
        max_length: Maximum length of the column (float due to .max() output).
    
    Returns:
        int: Conventional VARCHAR length (e.g., 50, 100, 255, 500, 1000, etc.).
    """
    if pd.isna(max_length):
        return 50
    
    max_length = int(max_length)
    conventional_lengths = [50, 100, 255, 500, 1000, 2000, 4000, 8000]
    
    for length in conventional_lengths:
        if max_length <= length:
            return length
    
    return ((max_length // 1000) + 1) * 1000

def calc_global_len(data) -> int:
    """
    Calculate the maximum length across all string columns and return the closest
    conventional VARCHAR length greater than or equal to it.
    
    Args:
        data: Pandas DataFrame.
    
    Returns:
        int: Single conventional VARCHAR length for all string columns.
    """
    # Identify string columns
    string_columns = data.select_dtypes(include=['object']).columns

    # Calculate the maximum length across all string columns
    max_length = 0
    for column in string_columns:
        column_max = data[column].astype(str).str.len().max()
        if pd.notna(column_max):  # Skip if all NaN
            max_length = max(max_length, int(column_max))

    # Get the conventional VARCHAR length for the global max
    varchar_length = get_conventional_length(max_length)

    # Print for verification (optional)
    print(f"Global maximum length across all string columns: {max_length}")
    print(f"Suggested VARCHAR length for all string columns: VARCHAR({varchar_length})")

if __name__ == "__main__":
    data = pd.read_csv("./data/legacy_27/legacy_healthcare_data.csv")
    # test_cutoff(data)
    # check_doc_departments(data)
    check_for_ids(data)
    # calc_global_len(data)
    pass