from abc import abstractmethod
from typing import Union, Dict, List

import pandas as pd

from src.app.open_fiscal.test import FiscalDataManager

pd.options.display.float_format = "{:,.0f}".format

class BaseAggregator(FiscalDataManager):
    """Base class for fiscal data aggregation."""

    def __init__(self, start_year: int, end_year: int):
        super().__init__(start_year=start_year, end_year=end_year)

    def aggregate(self, index: Union[str, List[str]], columns: Union[str, List[str]], values: str) -> pd.DataFrame:
        return self.data.pivot_table(index=index, columns=columns, values=values, aggfunc="sum")

class BudgetDataFormatter:
    """Analyzes budget changes and formats results."""

    @staticmethod
    def format_percentage(x: int | float) -> str:
        """Formats a number as a percentage string."""

        if isinstance(x, (int, float)):
            return f"+{x:.2f}%" if x > 0 else (f"{x:.2f}%" if x < 0 else "NaN")
        return "NaN"

    @staticmethod
    def sort_yearly_budgets(budget_data: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Sorts budget data by ministry for each year.

        Args:
            budget_data (pd.DataFrame): A DataFrame where rows are ministries
                                        and columns are fiscal years or a MultiIndex.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary with sorted DataFrames for each year.
                                     Keys include year with sorting order ('asc', 'desc').
        """
        sorted_budgets = {}

        # Check if columns are MultiIndex or single Index
        if isinstance(budget_data.columns, pd.MultiIndex):
            # Handle MultiIndex: Iterate over second level of the index (fiscal years)
            for year in budget_data.columns.get_level_values(1).unique():
                # Extract budget data for the specific year
                year_data = budget_data.xs(key=year, axis=1, level=1, drop_level=False)
                # Sort by ascending order
                sorted_budgets[f"{year}_asc"] = year_data.sort_values(
                    by=(budget_data.columns[0][0], year), ascending=True
                )
                # Sort by descending order
                sorted_budgets[f"{year}_desc"] = year_data.sort_values(
                    by=(budget_data.columns[0][0], year), ascending=False
                )
        else:
            # Handle single Index: Columns represent fiscal years
            for year in budget_data.columns:
                # Extract budget data for the specific year
                year_data = budget_data[[year]]
                # Sort by ascending order
                sorted_budgets[f"{year}_asc"] = year_data.sort_values(by=year, ascending=True)
                # Sort by descending order
                sorted_budgets[f"{year}_desc"] = year_data.sort_values(by=year, ascending=False)

        return sorted_budgets

    @staticmethod
    def calculate_yoy_change(df: pd.DataFrame) -> pd.DataFrame:
        """Calculates year-over-year percentage changes."""

        return (df.diff(axis=1) / df.shift(axis=1).replace(0, pd.NA)) * 100

class BudgetAnalysisService:
    """Provides budget analysis services."""

    def __init__(self, start_year: int, end_year: int):
        self.ministry_aggregator = BaseAggregator(start_year, end_year)
        self.change_formatter = BudgetDataFormatter()

    def get_ministry_budget_trends(self, index: List[str], columns: List[str], values: str) -> Dict[str, pd.DataFrame]:
        # Generate a pivot table
        budget_data_by_ministry = self.ministry_aggregator.aggregate(
            index=index, columns=columns, values=values
        )

        # Calculate year-over-year (YoY) changes
        changes_yoy = self.change_formatter.calculate_yoy_change(
            budget_data_by_ministry
        ).map(self.change_formatter.format_percentage)

        # Sort the aggregated data
        sorted_budgets = self.change_formatter.sort_yearly_budgets(budget_data_by_ministry)


        return {
            "budget_by_ministry": budget_data_by_ministry,
            "changes_yoy": changes_yoy,
            "sorted_budgets": sorted_budgets,
        }


service = BudgetAnalysisService(2024, 2025)
result = service.get_ministry_budget_trends(index=["OFFC_NM"], columns=["FSCL_YY"], values="Y_YY_MEDI_KCUR_AMT")

print(result["sorted_budgets"])
# 기존 변화율 저장
# result["changes_yoy"].to_csv("changes_yoy.csv", encoding="utf-8-sig", index=True)

# 정렬된 데이터 저장
# for year_order, data in result["sorted_budgets"].items():
#     data.to_csv(f"budget_{year_order}.csv", encoding="utf-8-sig", index=True)

# print("csv 저장완료.")
