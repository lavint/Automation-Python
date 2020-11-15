import pandas as pd
import glob
from datetime import datetime

# Initialize variables
path = "input/*.xlsx"
li = []


# For each file in the path, read into DataFrame and append each to a list
for fname in glob.glob(path):
    df = pd.read_excel(fname)
    li.append(df)
    
# Concat DataFrames in the list by row and remove rows that have NA in functional area
df_all_functions = pd.concat(li, axis=0, ignore_index=True) 
df_all_functions = df_all_functions[df_all_functions['Functional Area'].notna()]

# Read mapping file and select 3 columns
df_id_mapping = pd.read_excel("mapping.xlsx", sheet_name="Role-ID")
df_id_mapping = df_id_mapping[['Role Name', 'ID', 'ID Description']]

# Remove row if it contains certain words in the Role Name column
rm_ls = ['BATCH', 'CUTOVER', 'FUNCTIONAL', 'CONFIG', 'RESIDUAL']
rm_str = '|'.join(rm_ls)
df_id_mapping = df_id_mapping[~df_id_mapping['Role Name'].str.contains(rm_str, case=False)]

# Group all corresponding role names in one row per ID and ID Description
df_id_mapping = df_id_mapping.groupby(['ID', 'ID Description'])['Role Name'].apply(', '.join).reset_index()

# Merge DataFrames
df_roles = pd.merge(df_all_functions, df_id_mapping, how="left", left_on = "App ID", right_on = "ID")

# Put all rows with role ID into a DataFrame and the ones with no ID into another DataFrame
df_roles_exist = df_roles[df_roles['ID'].notna()].copy()
df_roles_need_research = df_roles[df_roles['ID'].isnull()].copy()


# For each function area, get the corresponding role names into a list
ls_all_unique_roles = []
for function in df_roles['Functional Area'].unique().tolist():
    role_list = df_roles.loc[df_roles['Functional Area'] == function, 'Role Name'].tolist()
    
    # Extend list roles by appending elements from list x
    roles = []
    for r in role_list:
        if type(r) != float:
            x = r.split(',')            
            roles.extend(x)
            
    # Remove duplicate roles in a function area        
    unique_roles = set(roles)
    ls_unique_roles = list(unique_roles)
    
    # Append series of unique roles into a list ls_all_unique_roles 
    # with the corresponding function as column header
    ls_all_unique_roles.append(pd.Series(ls_unique_roles, name=function))

    
# Concate list of series by column into a DataFrame   
df_all_unique_roles = pd.concat(ls_all_unique_roles, axis=1)

# Get today's date
today = datetime.today().strftime('%Y_%m_%d')

# Write DataFrames into sheets in an excel file
with pd.ExcelWriter(f'output_{today}.xlsx') as writer:
    df_roles_exist.to_excel(writer, sheet_name='Existing_Roles', index=False)
    df_roles_need_research.to_excel(writer, sheet_name='Need_Research_Roles', index=False)
    df_all_unique_roles.to_excel(writer, sheet_name='Unique_Roles_Per_Value_Stream', index=False)