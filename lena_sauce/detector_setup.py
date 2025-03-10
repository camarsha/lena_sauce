import sauce
import re
from tqdm import tqdm
import polars as pl
from typing import Union
import sys
import importlib.resources

nai_map = importlib.resources.files("lena_sauce") / "NaI-Map.csv"
scint_map = importlib.resources.files("lena_sauce") / "Scint-Map.csv"


def make_annulus(
    run_data,
    map_file=nai_map,
    union=False,
):
    """Create the annulus detector using the map file.

    :param map_file:
    :returns:

    """

    det_list = []

    with open(map_file, "r") as f:
        next(f)  # skip the header
        for line in tqdm(f):
            line = line.split()
            if line:
                temp = sauce.Detector(line[2])  # use the name
                temp.find_hits(
                    run_data, module=int(line[0]), channel=int(line[1])
                )  # find the events
                # drop useless stuff for now
                num = int(re.split("(\d+)", temp.name)[1])
                temp.tag(num, tag_name="nai_segment")
                temp.primary_energy_col = "long"
                det_list.append(temp)
    # now make a union and return
    if union:
        return sauce.detector_union("nai", *det_list)
    else:
        return det_list


def make_muon_veto(
    run_data,
    map_file=scint_map,
    muon_thresholds=None,
    union=False,
):
    """Create the muon veto detector using the map file.

    :param map_file:
    :returns:

    """

    det_list = []

    with open(map_file, "r") as f:
        next(f)  # skip the header
        for line in tqdm(f):
            line = line.split()
            if line:
                temp = sauce.Detector(line[2])  # use the name
                temp.find_hits(
                    run_data, module=int(line[0]), channel=int(line[1])
                )  # find the events
                num = int(re.split("(\d+)", temp.name)[1])
                temp.tag(num, tag_name="PS_segment")
                if muon_thresholds:
                    temp.apply_threshold(int(line[3]), col="long")
                det_list.append(temp)
    # now make a union and return
    if union:
        return sauce.detector_union("PS", *det_list)
    else:
        return det_list


def nai_sum(
    annulus: Union[sauce.Detector, list[sauce.Detector]],
    col,
    dt=5.0,
    default_name="nai",
) -> sauce.Detector:
    """Reduce the annulus
    by summing within a local event builder.

    :param annulus:
    :returns:

    """
    if isinstance(annulus, list):
        annulus = sauce.detector_union(default_name, *annulus)

    annulus.build_referenceless_events(dt)
    # TODO: Check that this is what you want
    annulus.data = (
        annulus.data.with_columns(pl.col(col).sum().over("event_nai"))
        .sort(["event_nai", "tdc"])
        .unique("event_nai", maintain_order=True, keep="first")
    )
    return annulus


def make_all_detectors(run_data):
    caller_globals = sys._getframe(1).f_globals
    nai = make_annulus(run_data)
    caller_globals["nai"] = nai
    scint = make_muon_veto(run_data)
    caller_globals["scint"] = scint
    hpge = sauce.Detector("hpge").find_hits(run_data, module=225, channel=0)
    caller_globals["hpge"] = hpge
    hpge_t = sauce.Detector("hpge_t").find_hits(run_data, module=225, channel=2)
    caller_globals["hpge_t"] = hpge_t
    pulser = sauce.Detector("pulser").find_hits(run_data, module=225, channel=4)
    caller_globals["pulser"] = pulser
    beam = sauce.Detector("beam").find_hits(run_data, module=225, channel=6)
    caller_globals["beam"] = beam
    return hpge, hpge_t, pulser, beam, nai, scint
