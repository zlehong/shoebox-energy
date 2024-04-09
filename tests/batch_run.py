import os
import sys
from pathlib import Path
import click
import json
import pandas as pd
import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from epjson.epjson_idf import EpJsonIDF
from epjson.simulate_cluster import SimulateCluster

if __name__ == "__main__":
    epw = "D:\\Users\\zoelh\\Dropbox (MIT)\\4.S42 Campus Decarb\\Energy Modeling\\epws\\USA_MA_Boston-Logan.Intl.AP.725090_TMY3.epw"
    weights_df = pd.read_json("D:\\Users\\zoelh\\Dropbox (MIT)\\4.S42 Campus Decarb\\Energy Modeling\\idfs\\umi\\shoebox-weights-newpaths.json")
    print(len(weights_df.ParentBuildingId.unique()))
    df = weights_df.groupby("ShoeboxPath").first().reset_index()

    # bid = "57f7cbb0-5996-4aff-a20e-18c2bf51f2aa"
    # test_paths = df[df.ParentBuildingId == bid]["ShoeboxPath"].to_list()
    
    idf_cluster = SimulateCluster(
        idf_list = df.ShoeboxPath.to_list(),
        epw = epw,
        weights_df = weights_df,
    )

    errors = idf_cluster.parallel_simulate()
    print(errors)

    bids = weights_df.ParentBuildingId.unique()
    r = idf_cluster.fetch_building_results_parallel(bids)
    bids = list(r.keys())
    all_results = r[bids[0]]
    for k in bids[1:]:
        all_results = pd.concat([all_results, r[k]], ignore_index=True)
    # SAVE TO HD5 WITH epw as key
    print(all_results.shape)
    all_results.to_hdf("D:\\Users\\zoelh\\Dropbox (MIT)\\4.S42 Campus Decarb\\Energy Modeling\\01_all_model_results.h5", key="BostonLoganTMY3")
    