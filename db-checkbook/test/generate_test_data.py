def generate_cpdb_test_data():
    cpdb_data_1 = gpd.GeoDataFrame({
        'maprojid': [1, 2, 3],
        'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
    })
    cpdb_data_2 = gpd.GeoDataFrame({
        'maprojid': [4, 5, 6],
        'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
    })
    cpdb_data_3 = gpd.GeoDataFrame({
        'maprojid': [7, 8, 9],
        'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
    })
    cpdb_data = [cpdb_data_1, cpdb_data_2, cpdb_data_3]
    return cpdb_data

def generate_checkbook_test_data():
    checkbook_data = pd.DataFrame({
        'FMS ID': [1, 2, 3],
        'Contract Purpose': ['Contract A', 'Contract B', 'Contract C'],
        'Agency': ['Agency X', 'Agency Y', 'Agency Z'],
        'Budget Code': ['Code 1', 'Code 2', 'Code 3'],
        'Check Amount': [1000, 2000, 3000]
    })
    return checkbook_data
