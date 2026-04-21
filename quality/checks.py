"""
Data quality rules engine.
Define checks as classes, run them against any DataFrame.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timezone
from enum import Enum


class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING  = "WARNING"
    INFO     = "INFO"


@dataclass
class CheckResult:
    check_name:   str
    passed:       bool
    severity:     Severity
    metric:       float
    threshold:    float
    message:      str
    rows_affected: int = 0
    timestamp:    str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class QualityCheck:
    name     = "base_check"
    severity = Severity.WARNING

    def run(self, df: pd.DataFrame) -> CheckResult:
        raise NotImplementedError


class NullCheck(QualityCheck):
    name = "null_check"

    def __init__(self, columns: List[str], threshold_pct: float = 1.0, severity=Severity.CRITICAL):
        self.columns   = columns
        self.threshold = threshold_pct
        self.severity  = severity

    def run(self, df: pd.DataFrame) -> CheckResult:
        null_counts = df[self.columns].isnull().sum().sum()
        total       = len(df) * len(self.columns)
        null_rate   = (null_counts / total * 100) if total > 0 else 0
        passed      = bool(null_rate <= self.threshold)

        return CheckResult(
            check_name    = self.name,
            passed        = passed,
            severity      = self.severity,
            metric        = round(null_rate, 4),
            threshold     = self.threshold,
            rows_affected = int(null_counts),
            message       = f"Null rate {null_rate:.2f}% {'✓' if passed else f'⚠ exceeds {self.threshold}%'}",
        )


class DuplicateCheck(QualityCheck):
    name = "duplicate_check"

    def __init__(self, key_columns: List[str], threshold_pct: float = 0.5, severity=Severity.CRITICAL):
        self.key_columns = key_columns
        self.threshold   = threshold_pct
        self.severity    = severity

    def run(self, df: pd.DataFrame) -> CheckResult:
        dupes     = df.duplicated(subset=self.key_columns).sum()
        dupe_rate = (dupes / len(df) * 100) if len(df) > 0 else 0
        passed    = bool(dupe_rate <= self.threshold)

        return CheckResult(
            check_name    = self.name,
            passed        = passed,
            severity      = self.severity,
            metric        = round(dupe_rate, 4),
            threshold     = self.threshold,
            rows_affected = int(dupes),
            message       = f"Duplicate rate {dupe_rate:.2f}% {'✓' if passed else f'⚠ exceeds {self.threshold}%'}",
        )


class RangeCheck(QualityCheck):
    name = "range_check"

    def __init__(self, column: str, min_val: float, max_val: float, severity=Severity.WARNING):
        self.column   = column
        self.min_val  = min_val
        self.max_val  = max_val
        self.severity = severity

    def run(self, df: pd.DataFrame) -> CheckResult:
        out_of_range  = ((df[self.column] < self.min_val) | (df[self.column] > self.max_val)).sum()
        violation_rate = (out_of_range / len(df) * 100) if len(df) > 0 else 0
        passed         = bool(out_of_range == 0)

        return CheckResult(
            check_name    = self.name,
            passed        = passed,
            severity      = self.severity,
            metric        = round(violation_rate, 4),
            threshold     = 0,
            rows_affected = int(out_of_range),
            message       = f"Range [{self.min_val},{self.max_val}]: {out_of_range} violations {'✓' if passed else '⚠'}",
        )


class FreshnessCheck(QualityCheck):
    name = "freshness_check"

    def __init__(self, timestamp_col: str, max_age_hours: float = 24, severity=Severity.WARNING):
        self.timestamp_col = timestamp_col
        self.max_age_hours = max_age_hours
        self.severity      = severity

    def run(self, df: pd.DataFrame) -> CheckResult:
        latest    = pd.to_datetime(df[self.timestamp_col]).max()
        age_hours = (datetime.now(timezone.utc) - latest.to_pydatetime().replace(tzinfo=timezone.utc)).total_seconds() / 3600
        passed    = bool(age_hours <= self.max_age_hours)

        return CheckResult(
            check_name    = self.name,
            passed        = passed,
            severity      = self.severity,
            metric        = round(age_hours, 2),
            threshold     = self.max_age_hours,
            message       = f"Data age {age_hours:.1f}h {'✓' if passed else f'⚠ exceeds {self.max_age_hours}h SLA'}",
        )


class AnomalyCheck(QualityCheck):
    name = "anomaly_check"

    def __init__(self, column: str, z_threshold: float = 3.0, max_pct: float = 2.0, severity=Severity.WARNING):
        self.column      = column
        self.z_threshold = z_threshold
        self.max_pct     = max_pct
        self.severity    = severity

    def run(self, df: pd.DataFrame) -> CheckResult:
        mean, std   = df[self.column].mean(), df[self.column].std()
        anomalies   = ((df[self.column] - mean).abs() > self.z_threshold * std).sum()
        anomaly_pct = (anomalies / len(df) * 100) if len(df) > 0 else 0
        passed      = bool(anomaly_pct <= self.max_pct)

        return CheckResult(
            check_name    = self.name,
            passed        = passed,
            severity      = self.severity,
            metric        = round(anomaly_pct, 4),
            threshold     = self.max_pct,
            rows_affected = int(anomalies),
            message       = f"Anomalies: {anomalies} rows ({anomaly_pct:.2f}%) {'✓' if passed else '⚠'}",
        )
