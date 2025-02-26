
#  * Program Name: Database System -> Milestone2 
#  * Author: Next Genrartion
#  * Date: Feb 23/2025 (Final_Version_v04)
#  * Description:
'''
    My disk utilites class is provide static methods to recurisvely coverting 
    the disctionary keys becmase the stirng and intger formats.

'''
class diskUtilities:
    '''
        convert_keys_to_str(obj) convert exmaple
        for example:
            if the data = {1: { 2: 'value', 3: [4, 5, {6: 'nested'}]}} 
        called convert_keys_to_str( data )
            then output: {'1': {'2': 'value', '3': [4, 5, {'6': 'nested'}]}}
    '''
    # @staticmethod
    def convert_keys_to_str(obj):
        if isinstance(obj, dict):
            # print(f"object: {obj}, dictionary: {dict}") # debugging 
            return {str(k): diskUtilities.convert_keys_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            # print(f"object: {obj}, list: {list}") # debugging 
            return [diskUtilities.convert_keys_to_str(item) for item in obj]
        else:
            return obj


    '''
        This function will convert obj key to intergers 
        For example: 
            if my data = {"1": {"2": "value", "3": [4, 5, {"6": "nested"}]}}
            output = {1: {2: 'value', 3: [4, 5, {6: 'nested'}]}} 
    '''
    # @staticmethod
    def convert_keys_to_int(obj):
        if isinstance(obj, dict):
            new_dict = {}
            for k, v in obj.items():
                try:
                    new_key = int(k)
                    # print(f"new key: {new_key}) # debugging 
                except (ValueError, TypeError):
                    new_key = k         # if the conversion fails to do, then keep the orginal key 
                new_dict[new_key] = diskUtilities.convert_keys_to_int(v)
            return new_dict
        elif isinstance(obj, list):
            # print(f"object: {obj}, list: {list}") # debugging 
            return [diskUtilities.convert_keys_to_int(item) for item in obj]
        else:
            return obj
