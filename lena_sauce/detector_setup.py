import sauce
import re
from tqdm import tqdm
import polars as pl
from typing import Union
import sys
import importlib.resources

nai_map = importlib.resources.files("lena_sauce") / "NaI-Map.csv"
scint_map = importlib.resources.files("lena_sauce") / "Scint-Map.csv"
hpge_map = importlib.resources.files("lena_sauce") / "HPGe-Map.csv"

nai_df = pl.read_csv(nai_map)
scint_df = pl.read_csv(scint_map)
hpge_df = pl.read_csv(hpge_map)


def make_annulus(
    run_data,
    map_df=nai_df,
    union=False,
):
    """Create the annulus detector using the map file.

    :param map_file:
    :returns:

    """

    det_list = []

    for row in tqdm(map_df.iter_rows(named=True)):
        temp = sauce.Detector(row["name"])
        temp.find_hits(
            run_data,
            module=int(row["module"]),
            channel=int(row["channel"]),
        )
        # drop useless stuff for now
        num = int(re.split("(\\d+)", temp.name)[1])
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
    map_df=scint_df,
    muon_thresholds=None,
    union=False,
):
    """Create the muon veto detector using the map file.

    :param map_file:
    :returns:

    """

    det_list = []

    for row in tqdm(map_df.iter_rows(named=True)):
        temp = sauce.Detector(row["name"])
        temp.find_hits(run_data, module=int(row["module"]), channel=int(row["channel"]))
        num = int(re.split("(\\d+)", temp.name)[1])
        temp.tag(num, tag_name="PS_segment")

        if muon_thresholds:
            # Assuming threshold value is in fourth column
            temp.apply_threshold(int(row[map_df.columns[3]]), col="long")
        det_list.append(temp)

    # now make a union and return
    if union:
        return sauce.detector_union("PS", *det_list)
    else:
        return det_list


def make_hpge(
    run_data,
    map_df=hpge_df,
):
    """This function picks up the rest of the detectors present in the system.

    :param run_data: Run data
    :param map_df: Polars DataFrame
    :returns: Dictionary of Detector objects.
    """

    det_dic = {}

    for row in tqdm(map_df.iter_rows(named=True)):
        temp = sauce.Detector(row["name"])
        temp.find_hits(run_data, module=int(row["module"]), channel=int(row["channel"]))
        det_dic[temp.name] = temp

    return det_dic


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
    dets = make_hpge(run_data)
    hpge = None
    hpge_t = None
    pulser = None
    beam = None
    if "hpge" in dets:
        hpge = dets["hpge"]
        caller_globals["hpge"] = hpge
    if "hpge_t" in dets:
        hpge_t = dets["hpge_t"]
        caller_globals["hpge_t"] = hpge_t
    if "pulser" in dets:
        pulser = dets["pulser"]
        caller_globals["pulser"] = pulser
    if "beam" in dets:
        beam = dets["beam"]
        caller_globals["beam"] = beam
    return hpge, hpge_t, pulser, beam, nai, scint
