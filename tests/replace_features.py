import os
import sys
from pathlib import Path
import click
import json
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from epjson.epjson_idf import EpJsonIDF

def get_features_df(epjson, features_map):
    features = epjson.fetch_features(features_map)
    features = pd.DataFrame.from_dict(features, orient="index")
    return features

# make a click function which accepts a number of simulations to run, a bucket name, and an experiment name
@click.command()
@click.option(
    "--csv_path", default=None, help="Path to features csv", prompt="Path to features csv"
)
@click.option(
    "--path", default=None, help=".epjson locations", prompt="Path to epjsons"
)

def main(csv_path, path):
    # "D:/Users/zoelh/Dropbox (MIT)/4.S42 Campus Decarb/Energy Modeling/idfs/umi/shoeboxes.csv"
    features_df = pd.read_csv(csv_path)
    with open("tests/template_features_map.json", "r") as f:
        features_map = json.load(f)
    cols = set(features_df.columns) & set(features_map.keys())
    print(cols)

    # epjson = EpJsonIDF.from_epjson(features_df.EpjsonPath[0])
    # epjson.replace_features(features_df.loc[0][cols], features_map)
    for i, row in features_df.iterrows():
        print(row.EpjsonPath)
        epjson = EpJsonIDF.from_epjson(row.EpjsonPath)
        epjson.replace_features(row[cols], features_map)
    #     features = row[features_map.keys()]
    #     features["BUILDING"] = path.name.split(".idf")[0]
    #     features = features.reset_index()
    #     all_features = pd.concat([all_features, features], ignore_index=True)
    # all_features.to_csv(out)
    
if __name__ == "__main__":
    # path = "D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf"
    # features = "D:/Users/zoelh/GitRepos/shoebox-energy/tests/template_features_map.json"
    main()
    #D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf