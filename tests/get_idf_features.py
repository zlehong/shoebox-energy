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
    "--path", default=None, help=".idf locations", prompt="Path to idf files"
)
@click.option(
    "--out", default=None, help="Path to save features summary", prompt="Path to save summary"
)
@click.option(
    "--features", 
    default="D:/Users/zoelh/GitRepos/shoebox-energy/tests/template_features_map.json", 
    help="Path to location of features map as a .json", 
    prompt="Path to features map"
)

def main(path, out, features):
    names = os.listdir(path)
    paths = [Path(path) / x for x in names if ".idf" in x]
    with open(features, "r") as f:
        features_map = json.load(f)
    epjson = EpJsonIDF(paths[0])
    all_features = get_features_df(epjson, features_map)
    all_features["BUILDING"] = names[0].split(".idf")[0]
    all_features = all_features.reset_index()
    for path in paths[1:]:
        epjson = EpJsonIDF(path)
        features = get_features_df(epjson, features_map)
        features["BUILDING"] = path.name.split(".idf")[0]
        features = features.reset_index()
        all_features = pd.concat([all_features, features], ignore_index=True)
    all_features.to_csv(out)
    
if __name__ == "__main__":
    # path = "D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf"
    # features = "D:/Users/zoelh/GitRepos/shoebox-energy/tests/template_features_map.json"
    main()
    #D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf