from pathlib import Path
import pandas as pd

_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'

# categorization:

def categorization_summarization_statistics(df, category):
    if category=='None':
        cat_geoms = df[(df['has_geometry']==True) & (df['final_category'].isna())]
    else:
        cat_geoms = df[(df['has_geometry']==True) & (df['final_category']==category)]
    
    n = df.shape[0]
    m = cat_geoms.shape[0]

    print("% projects joined to CPDB geometries and categorized as {}: {}".format(category, round((m/n)*100, 2)))
    print("% total money captured by Checkbook NYC represented by joined projects categorized as {}: {}".format(category, round((sum(cat_geoms['check_amount'])/sum(df['check_amount']))*100,2)))
    return 

def geometries_summarization_statistics(df):
    geoms = df[df['has_geometry']==True]
    n = df.shape[0]
    m = geoms.shape[0]

    print("---")
    print("Number of projects mapped to geometries: {}".format(m))
    print("Percent of projects mapped to geometries: {}".format(round((m/n)*100, 2)))
    print("Amount of money mapped to geometries: {}".format(round(sum(geoms['check_amount']), 2)))
    print("Percent of money mapped to geometries: {}".format(round((sum(geoms['check_amount'])/sum(df['check_amount'])))))
    return   

def categorization_run_summary_statistics():

    final_categorization_file = pd.read_csv(_curr_file_path.parent.parent / 'output' / 'temp_historical_spend.csv')
    cats = ['Fixed Asset', 'ITT, Vehicles, and Equipment', 'Lump Sum', 'None']

    for cat in cats:
        geometries_summarization_statistics(final_categorization_file)
        categorization_summarization_statistics(final_categorization_file, cat)
    return

if __name__ == "__main__":
    print("started summarization ...")
    categorization_run_summary_statistics()
    print('finished summarization ...')