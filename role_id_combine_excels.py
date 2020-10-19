import pandas as pd
import glob
from datetime import datetime

path = "input/*.xlsx"

li = []
for fname in glob.glob(path):
    df = pd.read_excel(fname)
    li.append(df)
df_all_functions = pd.concat(li, axis=0, ignore_index=True)    
df_all_functions = df_all_functions[df_all_functions['Functional Area'].notna()]

df_id_mapping = pd.read_excel("mapping.xlsx", sheet_name="Role-ID")
df_id_mapping = df_id_mapping[['Role Name', 'ID', 'ID Description']]
df_id_mapping = df_id_mapping.groupby(['ID', 'ID Description'])['Role Name'].apply(', '.join).reset_index()

df_roles = pd.merge(df_all_functions, df_id_mapping, how="left", left_on = "App ID", right_on = "ID")

df_roles_exist = df_roles[df_roles['ID'].notna()].copy()
df_roles_need_research = df_roles[df_roles['ID'].isnull()].copy()

ls_all_unique_roles = []
for function in df_roles['Functional Area'].unique().tolist():
    role_list = df_roles.loc[df_roles['Functional Area'] == function, 'Role Name'].tolist()
    roles = []

    for r in role_list:
        if type(r) != float:
            x = r.split(',')
            roles.extend(x)
    unique_roles = set(roles)
    ls_unique_roles = list(unique_roles)
    
    ls_all_unique_roles.append(pd.Series(ls_unique_roles, name=function))
    
df_all_unique_roles = pd.concat(ls_all_unique_roles, axis=1)

today = datetime.today().strftime('%Y_%m_%d')

with pd.ExcelWriter(f'output_{today}.xlsx') as writer:
    df_roles_exist.to_excel(writer, sheet_name='Existing_Roles', index=False)
    df_roles_need_research.to_excel(writer, sheet_name='Need_Research_Roles', index=False)
    df_all_unique_roles.to_excel(writer, sheet_name='Unique_Roles_Per_Value_Stream', index=False)