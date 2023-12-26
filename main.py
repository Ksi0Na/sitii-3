import pandas as pd
import explorator


def read_data(file_path: str) -> pd.DataFrame:
    """
    Read data from a CSV file.

    Parameters:
    - file_path: str, path to the CSV file.

    Returns:
    - pd.DataFrame, loaded DataFrame.
    """
    df = pd.read_csv(file_path)
    return df


def save_info_df(df: pd.DataFrame) -> None:
    """
    Save general information about the DataFrame to a CSV file.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    df_info = explorator.general_info.get_total(df)
    output_file_path = 'data/info_db.csv'
    df_info.to_csv(output_file_path, index=False)


def convert_columns_to_datetime(df: pd.DataFrame) -> None:
    """
    Convert the start_time and end_time columns to datetime format.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])


def describe_data(df: pd.DataFrame) -> None:
    """
    Display descriptive statistics of the DataFrame.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    # print(df.info())
    # print('\n\n----------------------------------------------\n\n')
    # print(df.head(12))
    # print('\n\n----------------------------------------------\n\n')
    # print(df['type'].value_counts())
    # print('\n\n----------------------------------------------\n\n')
    # print(df[['type', 'name']].value_counts())
    # print('\n\n----------------------------------------------\n\n')
    # print(df['type'].value_counts(normalize=True))
    # print('\n\n----------------------------------------------\n\n')
    print(df.describe())


def attack_type_distribution(df: pd.DataFrame) -> None:
    """
    Distribution of attack types by time of day.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    convert_columns_to_datetime(df)
    bins = [0, 6, 12, 18, 24]
    labels = ['Night', 'Morning', 'Afternoon', 'Evening']
    df['time_of_day'] = pd.cut(df['start_time'].dt.hour, bins=bins, labels=labels, right=False)
    attack_distribution = df.groupby(['type', 'time_of_day']).size().unstack(fill_value=0)
    print(attack_distribution)


def avg_events_and_crit_by_os(df: pd.DataFrame) -> None:
    """
    Average number of events and criticality by operating system types.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    grouped_data = df.groupby('assets_id')
    average_data = grouped_data.agg({'events_count': 'mean', 'crit_rate': 'mean'})
    print(average_data)


def process_incident_data(file_path: str) -> None:
    """
    Process incident data: calculate event-to-duration ratio, select incidents with low ratios.

    Parameters:
    - file_path: str, path to the CSV file.

    Returns:
    - None.
    """
    df = pd.read_csv(file_path)
    convert_columns_to_datetime(df)
    df['event_duration_ratio'] = df['events_count'] / (df['end_time'] - df['start_time']).dt.total_seconds()
    top5_lowest_ratios = df.nsmallest(5, 'event_duration_ratio')
    median_crit_rate = df['crit_rate'].median()
    selected_incidents = top5_lowest_ratios[top5_lowest_ratios['crit_rate'] > median_crit_rate]
    print(selected_incidents['id'], end='\n\n\n')
    selected_incidents_details = df.loc[df['id'].isin(selected_incidents['id'])]
    print(selected_incidents_details)


def correlation_table(df: pd.DataFrame) -> None:
    """
    Correlation table and maximum absolute correlation.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    user_access_levels = {
        "admin": 1,
        "user123": 0.4,
        "dbadmin": 0.8,
        "guest": 0.2,
        "developer": 0.7,
        "tester": 0.75,
        "analyst": 0.6,
        "operator": 0.5,
        "manager": 0.65,
        "consultant": 0.55,
    }

    # Create the 'user_access_levels_mean' column
    df['user_access_levels_mean'] = df['assets_id'].apply(
        lambda assets: sum(user_access_levels.get(user, 0) for user in assets.split(',')) / len(
            assets.split(',')) if assets else 0
    )

    # Group by incident type and calculate correlation
    grouped_df = df.groupby('type')[['events_count', 'crit_rate', 'user_access_levels_mean']].corr()

    # Display the correlation table
    print("Correlation Table:")
    print(grouped_df)

    # Find the maximum absolute correlation with the 'user_access_levels_mean' column
    max_correlation_with_user_access = grouped_df['user_access_levels_mean'].abs().droplevel(0).max()
    max_correlation_column = grouped_df['user_access_levels_mean'].abs().droplevel(0).idxmax()

    # Display information about the maximum correlation with 'user_access_levels_mean'
    print("\nMaximum absolute correlation with user_access_levels_mean:", max_correlation_with_user_access)
    print("Corresponding column:", max_correlation_column)


if __name__ == '__main__':
    file_path = 'data/incidents.csv'
    df = read_data(file_path)
    # save_info_df(df)
    describe_data(df)
    attack_type_distribution(df)
    avg_events_and_crit_by_os(df)
    process_incident_data(file_path)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    correlation_table(df)

    # print(df)
