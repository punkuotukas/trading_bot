"""
main module
"""
from data_manager import (
    data_helper,
    api_data_manager
)

DataHelper = data_helper.DataHelper()

def main():
    """
    main method
    """
    # api_data_manager.update_candles_for_existing_pairs()
    api_data_manager.update_candles_for_existing_pairs()

if __name__ == "__main__":
    main()
