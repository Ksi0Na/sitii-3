import pandas as pd
import explorator
import requests
from ast import literal_eval


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


def save_assets(df: pd.DataFrame) -> None:
    """
    Save general information about the DataFrame to a CSV file.

    Parameters:
    - df: pd.DataFrame, the original DataFrame.

    Returns:
    - None.
    """
    df_info = explorator.general_info.get_total(df)
    output_file_path = 'data/assets.csv'
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


def get_assets_os(a):
    """
    Get information about assets from the server.

    Parameters:
    - a: list, list of asset ids.

    Returns:
    - data: List of dictionaries containing asset information.
    """
    resp = requests.get('https://d5d9e0b83lurt901t9ue.apigw.yandexcloud.net/get-assets-by-id',
                        params={'assets-id': ','.join(map(str, a))})
    resp.raise_for_status()

    data = resp.json().get("result", [])
    return data


def avg_events_and_crit_by_os(df: pd.DataFrame):
    """
    Average number of events and criticality by operating system types.

    Parameters:
    - file_path: str, path to the CSV file.

    Returns:
    - None.
    """
    df = pd.read_csv(file_path)
    df['assets_id'] = df.assets_id.apply(literal_eval)
    df = df.explode('assets_id')

    list_unique = list(df['assets_id'].unique())
    info_1 = get_assets_os(list_unique[:len(list_unique) // 2])
    info_2 = get_assets_os(list_unique[len(list_unique) // 2:])
    info = info_1 + info_2

    def get_os(id):
        result = [x for x in info if x.get('id') == int(id)]
        return result[0]['os'] if result else None  # Возвращаем None, если список result пуст

    df['asset_os'] = df.assets_id.apply(get_os)
    average_data = df.groupby('asset_os').agg({'events_count': 'mean', 'crit_rate': 'mean'})
    print(average_data)
    return info


def process_incident_data(df: pd.DataFrame) -> None:
    """
    Process incident data: calculate event-to-duration ratio, select incidents with low ratios.

    Parameters:
    - file_path: str, path to the CSV file.

    Returns:
    - None.
    """
    df['start_time'] = pd.to_datetime(df.start_time)
    df['end_time'] = pd.to_datetime(df.end_time)
    df_copy = df
    df_copy['relation'] = df.apply(lambda x: x['events_count'] / (
        (x['end_time'] - x['start_time']).total_seconds()), axis=1)

    df_copy.sort_values('relation', inplace=True)
    top5_lowest_ratios = df.head(5)['crit_rate'].median()

    selected_incidents = df_copy.loc[df.crit_rate > top5_lowest_ratios]
    selected_incidents_details = selected_incidents.sort_values(by="crit_rate", ascending=False)

    print(selected_incidents_details, end='\n\n\n')


def correlation_table(df: pd.DataFrame, info) -> None:
    """
    Correlation table and maximum absolute correlation.

    Parameters:
    - df: pd.DataFrame, исходный DataFrame.
    - info: List[Dict[str, Union[str, int, List[int]]]], информация об активах.

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

    def get_level_access_by_asset_id(assets_id):
        users_levels = []
        for asset_id in assets_id:
            result = [x for x in info if x['id'] == int(asset_id)]
            if result:
                user = result[0]['account_name']
                users_levels.append(user_access_levels.get(user, 0))
        return sum(users_levels) / len(users_levels) if users_levels else 0

    # Применить к каждому элементу списка assets_id функцию literal_eval
    df['assets_id'] = df.assets_id.apply(literal_eval)

    # Создать новый столбец 'user_access_levels_mean'
    df['user_access_levels_mean'] = df.assets_id.apply(get_level_access_by_asset_id)

    # Сгруппировать по типу инцидента и вычислить корреляции
    corr_df = df.groupby('type')[['events_count', 'crit_rate', 'user_access_levels_mean']].corr(method='pearson')

    # Вывести корреляционную таблицу
    print("Таблица корреляций:")
    print(corr_df)

    # Удалить строки с корреляцией -1 и 1
    corr_df = corr_df[
        (corr_df['user_access_levels_mean'] != -1.0) &
        (corr_df['user_access_levels_mean'] != 1.0)
    ]

    # Найти максимальное абсолютное значение корреляции
    max_correlation_with_user_access = corr_df['user_access_levels_mean'].abs().max()

    # Найти столбец с максимальной корреляцией
    max_correlation_column = corr_df[
        (corr_df['user_access_levels_mean'].abs() == max_correlation_with_user_access)
    ]

    # Вывести информацию о максимальной корреляции с 'user_access_levels_mean'
    print("\nМаксимальная абсолютная корреляция с user_access_levels_mean:", max_correlation_with_user_access)
    print("Соответствующий столбец:\n", max_correlation_column)


if __name__ == '__main__':
    file_path = 'data/incidents.csv'
    df = read_data(file_path)
    save_assets(df)
    describe_data(df)
    attack_type_distribution(df)

    info = avg_events_and_crit_by_os(df)
    process_incident_data(df)

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    correlation_table(df, info)




    # print(df)
