import unittest
import os 
from src import combine
import pandas as pd
import shutil

class functional_testing(unittest.TestCase):

    def setUp(self):
        self.not_exists_path="/abc/abc"
        self.empty_location_path="/abc/xyz"
        if not os.path.exists(self.empty_location_path):
            os.makedirs(self.empty_location_path)
        self.location_path_without_csv="/abc/notcsv"
        if not os.path.exists(self.location_path_without_csv):
            os.makedirs(self.location_path_without_csv)
        file = self.location_path_without_csv+'/test.txt'
        open(file, 'a').close()
        self.location_path_with_csv="/abc/csv"
        if not os.path.exists(self.location_path_with_csv):
            os.makedirs(self.location_path_with_csv)
        file = self.location_path_with_csv+'/test.csv'
        open(file, 'a').close()
        file = self.location_path_with_csv+'/abc 2.csv'
        open(file, 'a').close()

    def tearDown(self):
        shutil.rmtree("/abc")

    def __create_combined_test_folder(self):
        self.location_path_for_combined="/abc/combined"
        if not os.path.exists(self.location_path_for_combined):
            os.makedirs(self.location_path_for_combined)
        test_data1 = pd.DataFrame([['4.4.4.4', '2', '0'], ['4.4.4.4', '5', '100']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data1.to_csv(self.location_path_for_combined+'/asia 1.csv', index = False)
        test_data2 = pd.DataFrame([['4.4.4.4', '6', '0'], ['4.4.4.2', '5', '10']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data2.to_csv(self.location_path_for_combined+'/asia 2.csv', index = False)
        test_data3 = pd.DataFrame([['4.4.4.4', '6', '0'], ['4.4.4.2', '5', '10']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data3.to_csv(self.location_path_for_combined+'/na.csv', index = False)

    def __update_combined_test_folder(self):
        self.location_path_for_update="/abc/update"
        if not os.path.exists(self.location_path_for_update):
            os.makedirs(self.location_path_for_update)
        test_data1 = pd.DataFrame([['4.4.4.4', '2', '0'], ['4.4.4.4', '5', '100']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data1.to_csv(self.location_path_for_update+'/asia 1.csv', index = False)
        test_data2 = pd.DataFrame([['4.4.4.4', '6', '0'], ['4.4.4.2', '5', '10']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data2.to_csv(self.location_path_for_update+'/asia 2.csv', index = False)
        test_data3 = pd.DataFrame([['4.4.4.4', '6', '0'], ['4.4.4.2', '5', '10']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data3.to_csv(self.location_path_for_update+'/na.csv', index = False)
        combined_test_data = pd.DataFrame([['4.4.4.4', 'asia'], ['4.4.4.2', 'asia'], ['4.4.4.4', 'na'], ['4.4.4.2', 'na']], columns=['Source IP', 'Environment'])
        combined_test_data.to_csv(self.location_path_for_update+'/combined.csv', index = False)
        test_data3 = pd.DataFrame([['4.4.4.3', '6', '0'], ['4.4.4.1', '5', '10']], columns=['Source IP', 'Count', 'Events per Second'])
        test_data3.to_csv(self.location_path_for_update+'/na 1.csv', index = False)

    def test_location_doesnt_exists(self):
        self.assertFalse(combine._check_location_exists(self.not_exists_path))
    
    def test_location_exists_but_empty(self):
        self.assertTrue(combine._check_location_exists(self.empty_location_path))
        self.assertTrue(combine._check_folder_empty(self.empty_location_path))
    
    def test_location_exists_have_files_not_csv(self):
        self.assertTrue(combine._check_location_exists(self.location_path_without_csv))
        self.assertTrue(combine._check_folder_empty(self.location_path_without_csv))
    
    def test_location_exists_have_csv_files(self):
        self.assertTrue(combine._check_location_exists(self.location_path_with_csv))
        self.assertFalse(combine._check_folder_empty(self.location_path_with_csv))
    
    def test_get_file_name_with_env_without_combined(self):
        self.__create_combined_test_folder()
        test_dict_data = {'asia': ['asia 1.csv', 'asia 2.csv'], 'na': ['na.csv']}
        orig_dict = combine._get_file_name_with_env(self.location_path_for_combined)
        shutil.rmtree(self.location_path_for_combined)
        self.assertEqual(orig_dict, test_dict_data)
    
    def test_get_file_name_with_env_with_combined(self):
        self.__update_combined_test_folder()
        test_dict_data = {'na': ['na 1.csv']}
        orig_dict = combine._get_file_name_with_env(self.location_path_for_update)
        shutil.rmtree(self.location_path_for_update)
        self.assertEqual(orig_dict, test_dict_data)

    def test_concat_dataframe_concat(self):
        temporary_df1 = pd.DataFrame({"Source IP": ["0.0.0.0","0.0.0.111"], "Environment": ["NA Prod", "NA Prod"]})
        temporary_df2 = pd.DataFrame({"Source IP": ["0.2.021.20","10.02.04.111"], "Environment": ["Asia_prod", "Asia_prod"]})
        temp_merge = pd.DataFrame({"Source IP": ["0.0.0.0","0.0.0.111","0.2.021.20","10.02.04.111"], 
                                   "Environment": ["NA Prod", "NA Prod", "Asia_prod", "Asia_prod"]})

        output = combine._combine_cvs(temporary_df1,temporary_df2)
        self.assertTrue(temp_merge.equals(output))
    
    def test_data_traverse_function(self):
        self.__create_combined_test_folder()
        combine._data_transverse(self.location_path_for_combined)
        test_dataframe_data = pd.DataFrame([['4.4.4.4', 'asia'], ['4.4.4.2', 'asia'], ['4.4.4.4', 'na'], ['4.4.4.2', 'na']], columns=['Source IP', 'Environment'])
        dataframe=pd.read_csv(os.path.join(self.location_path_for_combined, "combined.csv") ,index_col=None)
        shutil.rmtree(self.location_path_for_combined)
        self.assertTrue(test_dataframe_data.equals(dataframe))
    
    def test_data_traverse_function_update(self):
        self.__update_combined_test_folder()
        combine._data_transverse(self.location_path_for_update)
        test_dataframe_data = pd.DataFrame([['4.4.4.4', 'asia'], ['4.4.4.2', 'asia'], ['4.4.4.4', 'na'], ['4.4.4.2', 'na'], ['4.4.4.3', 'na'], ['4.4.4.1', 'na']], columns=['Source IP', 'Environment'])
        dataframe=pd.read_csv(os.path.join(self.location_path_for_update, "combined.csv") ,index_col=None)
        shutil.rmtree(self.location_path_for_update)
        self.assertTrue(test_dataframe_data.equals(dataframe))
        
