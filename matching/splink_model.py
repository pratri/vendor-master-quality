"""Splink 5 (Fellegi-Sunter probabilistic linkage, DuckDB backend) for comparison with rapidfuzz.

Unsupervised (u from random sampling, m from EM). Same blocking keys as matching.blocking,
so both methods score the same pairs. Name term frequency down-weights common names.
"""

import logging

import pandas as pd
import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on

from matching.blocking import block_keys


def splink_scores(v: pd.DataFrame, seed: int = 42, use_city: bool = False) -> pd.DataFrame:
    # city off: double counts location with address + zip5 (dev F1 0.661 vs 0.669)
    """Match weight (log2 odds) and probability for each blocked pair."""
    keys = block_keys(v)
    df = pd.DataFrame({
        "unique_id": v["LIFNR"],
        "name_norm": v["name_norm"],
        "address_norm": v["address_norm"],
        "city": v["city"],
        "zip5": v["zip5"],
        "area": keys["area"],
        "first_token": keys["first_token"],
    }).replace("", None)

    settings = SettingsCreator(
        link_type="dedupe_only",
        blocking_rules_to_generate_predictions=[block_on("area"), block_on("first_token")],
        comparisons=[
            cl.JaroWinklerAtThresholds("name_norm", [0.95, 0.88, 0.8])
            .configure(term_frequency_adjustments=True),
            cl.JaroWinklerAtThresholds("address_norm", [0.95, 0.85]),
            *([cl.ExactMatch("city")] if use_city else []),
            cl.ExactMatch("zip5"),
        ],
        retain_matching_columns=False,
    )
    db_api = DuckDBAPI()
    linker = Linker(db_api.register(df, dataset_display_name="vendors"), settings,
                    log_level=logging.WARNING)
    linker.training.estimate_probability_two_random_records_match(
        [block_on("name_norm", "zip5")], recall=0.7)
    linker.training.estimate_u_using_random_sampling(max_pairs=2e6, seed=seed)
    # Train on address and name blocks. Training on zip5 learned "same town" instead of
    # "same company" (exact-name m came out near 0.03).
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("address_norm"))
    linker.training.estimate_parameters_using_expectation_maximisation(block_on("name_norm"))

    pred = linker.inference.predict().as_pandas_dataframe()
    a, b = pred["unique_id_l"], pred["unique_id_r"]
    return pd.DataFrame({"LIFNR_A": a.where(a < b, b), "LIFNR_B": b.where(a < b, a),
                         "match_weight": pred["match_weight"],
                         "match_probability": pred["match_probability"]})
