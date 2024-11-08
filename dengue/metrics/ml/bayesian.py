"""Module stores metrics functions."""

from typing import List

from properscoring import crps_ensemble


def crps(obs, pred) -> List[float]:
    """Compute CRPS for each forecast time."""
    score = crps_ensemble(obs, pred)
    return score.tolist()
