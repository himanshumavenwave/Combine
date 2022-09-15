import os
import pandas as pd
import re

def _saving_output(df, location):
    """
    Saves the dataframe as csv in specific directory
    Args:
        df (_type_): dataframe
        location (_type_): string
    """
    if os.path.exists(os.path.join(location,"combined.csv")):
        dataframe=pd.read_csv(os.path.join(location,"combined.csv") ,index_col=None)
        df = _combine_cvs(dataframe, df)
    df.drop_duplicates(subset=['Source IP', 'Environment'], inplace=True)
    df.to_csv(location+"/combined.csv", index = False)

def _combine_cvs(df_1, df_2):
    """
    Combine the dataframes together
    Args:
        df_1 (_type_): dataframe
        df_2 (_type_): dataframe
    Returns:
        _type_: dataframe
    """
    merge_df = pd.concat([df_1,df_2],ignore_index = True)
    return merge_df


def _get_file_name_with_env(location):
    """
    Creates a Dict with key as Env name and value as list of files who has that environment name.
    Only returns files that are added/modified after last run.
    Args:
        location (_type_): string
    Returns: 
        _type_: dict 
    """
    file_names = [file_name for file_name in os.listdir(location)  if file_name.endswith(".csv")]
    final_dict={}
    combined_edit_time = None
    if os.path.exists(location+"/combined.csv"):
        combined_edit_time = os.path.getctime(location+"/combined.csv")
    for file_name in file_names:
        if file_name == "combined.csv":
            continue
        if(combined_edit_time is None or combined_edit_time<os.path.getctime(location+"/"+file_name)):
            file_name_without_extention = file_name.split(".")[0].strip()
            key = ""
            if(re.search('[0-9]+$', file_name_without_extention)!=None):
                for i in range(len(file_name_without_extention)-1,0,-1):
                    if(not file_name_without_extention[i].isdigit()):
                        if( file_name_without_extention[i] == " "):
                            key = file_name_without_extention[0:i]
                            break
                        else:
                            key = file_name_without_extention[0:i+1]
                            break
            else:
                key=file_name_without_extention
            if key in final_dict:
                final_dict[key].append(file_name)
            else:
                final_dict[key] = [file_name,]

    return final_dict

def _generate_combined_cvs(file_name_with_type, location):
    """
    Generate combined dataframe
    Args:
        file_name_with_type (_type_): dict
    Returns:
        _type_: dataframe
    """
    final_cvs = None
    for k,v in file_name_with_type.items():
        same_type=[]
        for filename in v:
            dataframe=pd.read_csv(os.path.join(location,filename), index_col=None)
            dataframe.rename(columns={dataframe.columns[0]: 'Source IP'}, inplace=True)
            same_type.append(dataframe["Source IP"])
        same_type = pd.DataFrame(pd.concat(same_type))
        same_type["Environment"] = k
        if(final_cvs is not None):
            final_cvs = _combine_cvs(final_cvs, same_type)
        else:
            final_cvs = same_type
    return final_cvs

def _check_location_exists(location):
    """
    Checks if location exists. Return True if exists else False
    Args:
        location (_type_): string
    Returns:
        _type_: boolean
    """
    if not os.path.exists(location):
        return False
    return True

def _check_folder_empty(location):
    """
    Checks if folder on location is empty. Return True if empty else False
    Args:
        location (_type_): string
    Returns:
        _type_: boolean
    """
    if len([file_name for file_name in os.listdir(location) if file_name.endswith(".csv")])==0:
        return True
    return False

def _data_transverse(location):
    file_name_with_type = _get_file_name_with_env(location)
    final_cvs = _generate_combined_cvs(file_name_with_type, location)
    _saving_output(final_cvs, location)

if __name__ == "__main__":
    location = input('Enter Location\n')
    if not _check_location_exists(location):
        print("Location doesn't exists")
        exit()
    if _check_folder_empty(location):
        print("Location is empty")
        exit()
    try:
        _data_transverse(location)
        print ("Your programme has been executed successfully")
    except Exception as e:
        print("Program errored out with exception "+ e)