"""
This module is intended to give a basic way to energy calibrate
the NaI annulus.

- CAM 2026
"""

from dataclasses import dataclass
from math import ceil, floor
import matplotlib.pyplot as plt
from typing import Iterable, Sequence
import sauce
import numpy as np
from typing import Any, Optional, Type, Sequence, Union, List, Tuple
from scipy.optimize import curve_fit
import polars as pl
from tqdm import tqdm


@dataclass
class Region:
    lower: float
    upper: float

    def generate_region_for_hist(self):
        """Returns a integer region so that the counts
        can be histogrammed correctly.

        self.lower will be given by floor
        self.upper will be given by ceiling

        :returns: Tuple[int, int]

        """
        return floor(self.lower), ceil(self.upper)


@dataclass
class PeakParam:
    detector_name: str
    detector_col: str
    bg1: Region
    bg2: Region
    peak: Region
    energy: float
    denergy: float

    def print_out(self):
        return (
            f"{self.detector_name},{self.detector_col},"
            f"{self.bg1.lower},{self.bg1.upper},"
            f"{self.bg2.lower},{self.bg2.upper},"
            f"{self.peak.lower},{self.peak.upper},"
            f"{self.energy},{self.denergy}"
        )


def method_a_fit(
    det: sauce.Detector,
    bg1: Sequence[int],
    bg2: Sequence[int],
    peak: Sequence[int],
    col: Optional[str] = None,
):
    """Extract the peak parameters using Method A from Rodgers 2021.

    :param det: detector object
    :param bg1: background region 1
    :param bg2: background region 2
    :param peak: peak region
    :returns: centroid, centroid uncertainty, area, area uncertainty

    """
    len1 = bg1[1] - bg1[0]
    len2 = bg2[1] - bg2[0]
    len_peak = peak[1] - peak[0]

    bg1_x, bg1_y = det.hist(*bg1, len1, col)
    bg2_x, bg2_y = det.hist(*bg2, len2, col)
    peak_x, peak_y = det.hist(*peak, len_peak, col)

    # first work on estimating the background
    # mean counts in region
    B1 = bg1_y.mean()
    B2 = bg2_y.mean()

    # mean location
    x1 = bg1_x.mean()
    x2 = bg2_x.mean()

    # Now the background estimate
    B = ((B2 - B1) / (x2 - x1)) * (peak_x - x1) + B1
    # Signal Count Array
    T = peak_y
    S = T - B

    # area values
    area = S.sum()
    darea = np.sqrt(T.sum() + B.sum())

    # centroid: Note that this can nan if there are a lot of negative counts!
    cent = 1 / area * np.sum(S * peak_x)
    cent_var = (1 / (area - 1)) * np.sum(S * (peak_x - cent) ** 2.0)
    dcent = np.sqrt(cent_var / area)

    return cent, dcent, area, darea


def energy_cal(points: List[Tuple[float, float, float]]):
    """A very naive energy calibration for annulus calibrations.

    :param points: List of tuples(x_measured, E_cal, dE_cal)
    :param degree: degree of polynomial to fit.

    """
    x = [p[0] for p in points]
    y = [p[1] for p in points]
    dy = [p[2] for p in points]

    def model(x, a, b):
        return a * x + b

    params, _ = curve_fit(model, x, y, sigma=dy, absolute_sigma=True)
    return params[0], params[1]


def fit_peak(det: sauce.Detector, peak_params: PeakParam):
    bg1 = peak_params.bg1.generate_region_for_hist()
    bg2 = peak_params.bg2.generate_region_for_hist()
    peak = peak_params.peak.generate_region_for_hist()
    return method_a_fit(det, bg1, bg2, peak, col=peak_params.detector_col)


def calibrate_annulus_from_peaks(
    annulus_list: List[sauce.Detector], peak_file: str
):
    det_dic = {det.name: det for det in annulus_list}
    p_dic = {det.name: [] for det in annulus_list}
    cols = {}
    info = pl.read_csv(peak_file)

    # This does all the peak fitting
    for row in info.iter_rows(named=True):
        pp = PeakParam(
            row["name"],
            row["col"],
            Region(row["bg1_l"], row["bg_u"]),
            Region(row["bg2_l"], row["bg2_u"]),
            Region(row["p_l"], row["p_u"]),
            row["e"],
            row["de"],
        )
        cent, _, _, _ = fit_peak(det_dic[pp.detector_name], pp)
        p_dic[pp.detector_name].append((cent, pp.energy, pp.denergy))
        cols[pp.detector_name] = pp.detector_col
    # This does the curve fitting and calibration
    for key in tqdm(det_dic.keys()):
        if p_dic[key]:
            a, b = energy_cal(p_dic[key])
            det_dic[key]["energy"] = a * det_dic[key][cols[key]] + b


def plot_with_channel_and_energy(
    x_channel,
    y,
    a,
    b,
    energy_label="Energy (keV)",
    channel_label="Channel #",
    y_label="Count/bin",
):
    """Plot data with energy on bottom and raw x on top."""
    # Calculate energy from raw x: E = a*x + b

    fig, ax = plt.subplots()
    ax.plot(x_channel, y)  # Plot against energy

    # Set bottom axis (channel) labels
    ax.set_xlabel(channel_label)
    ax.set_ylabel(y_label)

    # Create top axis showing raw x values
    ax_top = ax.secondary_xaxis(
        "top",
        functions=(
            lambda x: a * x + b,
            lambda e: (e - b) / a,
        ),
    )
    ax_top.set_xlabel(energy_label)

    return ax
