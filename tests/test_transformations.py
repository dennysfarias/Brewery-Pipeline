import pytest
import pandas as pd
import os
import shutil
import sys

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from transformations import process_bronze_to_silver, process_silver_to_gold

@pytest.fixture
def setup_data():
    # Setup temporary directories
    bronze_file = "tests/temp_bronze.json"
    silver_root = "tests/temp_silver"
    gold_file = "tests/temp_gold/summary.parquet"
    
    # Create dummy bronze data
    data = [
        {"name": "Brewery A", "brewery_type": "micro", "state": "California", "city": "San Diego"},
        {"name": "Brewery B", "brewery_type": "micro", "state": "California", "city": "Los Angeles"},
        {"name": "Brewery C", "brewery_type": "large", "state": "New York", "city": "New York"},
        {"name": "Brewery D", "brewery_type": "micro", "state": None, "city": "Unknown"}, # Test missing state
    ]
    pd.DataFrame(data).to_json(bronze_file, orient='records')
    
    yield bronze_file, silver_root, gold_file
    
    # Teardown
    if os.path.exists(bronze_file):
        os.remove(bronze_file)
    if os.path.exists(silver_root):
        shutil.rmtree(silver_root)
    if os.path.exists(os.path.dirname(gold_file)):
        shutil.rmtree(os.path.dirname(gold_file))

def test_bronze_to_silver(setup_data):
    bronze_file, silver_root, _ = setup_data
    
    process_bronze_to_silver(bronze_file, silver_root)
    
    # check if parquet files exist
    assert os.path.exists(silver_root)
    
    # Verify partitioning (folder structure should contain state=California, state=New York, state=Unknown)
    assert os.path.exists(os.path.join(silver_root, "state=California"))
    assert os.path.exists(os.path.join(silver_root, "state=New York"))
    assert os.path.exists(os.path.join(silver_root, "state=Unknown")) # handled NaN

def test_silver_to_gold(setup_data):
    bronze_file, silver_root, gold_file = setup_data
    
    # Run silver first
    process_bronze_to_silver(bronze_file, silver_root)
    
    # Run gold
    process_silver_to_gold(silver_root, gold_file)
    
    assert os.path.exists(gold_file)
    
    # Load gold and verify aggregation
    df = pd.read_parquet(gold_file)
    
    # Expected: 
    # micro, California: 2
    # large, New York: 1
    # micro, Unknown: 1
    
    cal_micro = df[(df['state'] == 'California') & (df['brewery_type'] == 'micro')]
    assert len(cal_micro) == 1
    assert cal_micro.iloc[0]['brewery_count'] == 2

    ny_large = df[(df['state'] == 'New York') & (df['brewery_type'] == 'large')]
    assert len(ny_large) == 1
    assert ny_large.iloc[0]['brewery_count'] == 1
