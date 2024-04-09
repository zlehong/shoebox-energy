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
    "--path", default=None, help="Path to features csv", prompt="Path to features csv"
)

def main(path):
    # "D:/Users/zoelh/Dropbox (MIT)/4.S42 Campus Decarb/Energy Modeling/idfs/umi/shoeboxes.csv"
    features_df = pd.read_csv(path)
    with open("tests/template_features_map.json", "r") as f:
        features_map = json.load(f)
    with open("tests/schedule_bunches.json", "r") as f:
        schedules = json.load(f)
    cols = set(features_df.columns) & set(features_map.keys())
    df = features_df.groupby("ShoeboxPath").first().reset_index()

    # TESTS
    # df = features_df[features_df.ParentBuildingId == "57f7cbb0-5996-4aff-a20e-18c2bf51f2aa"]
    # df = features_df[features_df.ParentBuildingId == "c05749ba-04be-4a37-ae37-2e62b15527c7"]

    # epjson = EpJsonIDF.from_epjson(features_df.EpjsonPath[0])
    # epjson.replace_features(features_df.loc[0][cols], features_map)
    # epjson.remove("ZoneVentilation:DesignFlowRate")
    # epjson.save()


    for i, row in df.iloc[:].iterrows():
        if not os.path.isfile(Path(row.EpjsonPath).parent / "eplusout.sql"):
            try:
                epjson = EpJsonIDF.from_epjson(row.EpjsonPath)
                epjson.replace_features(row[cols], features_map)
                # epjson.remove("ZoneVentilation:DesignFlowRate")
                epjson.update_schedules(schedules)
                epjson.add([(
                    "Output:SQLite",
                    {"Output:SQLite 1": {"option_type": "Simple"}}
                    )])
                epjson.epjson["ZoneInfiltration:DesignFlowRate"]["CoreInfiltration"].update(
                    {"air_changes_per_hour": 0}
                )
                epjson.save()
                epjson.save_idf()
                print("Completed", Path(row.EpjsonPath))
            except Exception as e:
                print(f"FAILED ON INDEX {i}")
                raise e

    
if __name__ == "__main__":
    # path = "D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf"
    # features = "D:/Users/zoelh/GitRepos/shoebox-energy/tests/template_features_map.json"
    main()
    #D:/Users/zoelh/GitRepos/mit-energy-model/eplus_server/EnergyPlusSim/idf

#TODO: test with ventilation sched as all on and occupancy 