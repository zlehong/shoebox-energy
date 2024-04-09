# Test convert class method from a list of idfs
import os
import sys
from pathlib import Path
import click
import json
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from epjson.epjson_idf import EpJsonIDF

@click.command()
@click.option(
    "--csv_path", 
    help="Path to location of csv with list of names", 
    prompt="Path to features map"
)
@click.option(
    "--col", 
    default="ShoeboxPath",
    help="Path to location of csv with list of names", 
    prompt="Path to features map"
)
@click.option(
    "--type", 
    default="epjson",
)

def main(csv_path, col, type):
    df = pd.read_csv(csv_path) #D:\\Users\\zoelh\\Dropbox (MIT)\\4.S42 Campus Decarb\\Energy Modeling\\idfs\\umi\\shoeboxes2.csv
    print(df.shape)
    df = df.groupby("ShoeboxPath").first().reset_index()
    print(df.shape)
    # df = df[df.ParentBuildingId == "57f7cbb0-5996-4aff-a20e-18c2bf51f2aa"]
    idfs = df[col].to_list()
    print(len(idfs))
    eplus_location = Path("C:\EnergyPlusV22-2-0")
    files_and_dirs = [(x, str(Path(x).parent)) for x in idfs]
    EpJsonIDF.parallel_convert(files_and_dirs, eplus_location, file_type=type)

if __name__ == "__main__":
    main()