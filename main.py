"""
main module
"""
from data_manager import (
    data_helper,
    api_data_manager,
    status_helper,
)

DataHelper = data_helper.DataHelper()
StartTimeFinder = start_time_finder.StartTimeFinder()
APIDataManager = api_data_manager.APIDataManager()

def main():
    """
    main method
    """
    # status_helper.check_for_new_pairs()
    # status_helper.update_disabled_pairs()
    print(DataHelper.retrieve_pairs_without_start_timestamp)
    APIDataManager.find_starting_timestamp_for_new_pairs()
    # api_data_manager.update_candles_for_existing_pairs()

if __name__ == "__main__":
    main()
