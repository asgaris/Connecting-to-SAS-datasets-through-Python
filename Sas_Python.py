import pandas as pd
import saspy

#Read two tables from sas and merge them based on common columns
def get_data_from_sas ():
    sascfgfile = r"cfgfile location"
    conn = saspy.SASsession (cfgfile = sascfgfile, cfgname = 'winiom')
    table_one = conn.sasdata ('sas_table_one', 'sas_library_one').to_df ()
    table_two = conn.sasdata ('sas_table_two', 'sas_library_two').to_df ()
    return table_one, table_two

table_one, table_two = get_data_from_sas ()

#Read some columns of table_one and table_two
table_one = table_one [['report_date', 'client_id', 'age', 'postal_code']].rename(columns={'report_date':'date'})
table_two = table_two [['city', 'income', 'postal_code']]

#Merge table_one and table_two
merge_table = pd.merge(table_one, table_two, on=['postal_code'], how='inner').drop([])


#Filter data for age above 20 and date above '2021-01-01'
merge_table = merge_table [merge_table['age'] > 20]
start_date = '2021-01-01'
merge_table = merge_table [merge_table['date'] > start_date]

#Sort merge_table by date
merge_table = merge_table.sort_values(by='date', ascending=True)

#Export as csv file
merge_table.to_csv('merge_table.csv')