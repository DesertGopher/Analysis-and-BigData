import inspect
from pathlib import Path
from typing import Any

import pandas as pd
import pylab
from scipy.stats import ttest_ind
from tabulate import tabulate

CSV_FILE_PATH = (
    Path(__file__).resolve().parent / "marketing_AB.csv"
)


class ABTesting:
    """
    Initializes the object with the given marketing_AB dataframe.
    Args:
        dataframe (pd.DataFrame): The dataframe to be used for initialization.
    """

    def __init__(self, dataframe: pd.DataFrame):
        self.df = dataframe

    @staticmethod
    def print_df(*dataframes: pd.DataFrame | Any):
        """
        Print the given DataFrame using tabulate to format the output.
        If tabulate raises a TypeError, print the DataFrame as is.
        Finally, print a new line.
        """
        print(f"<<<<< {inspect.stack()[1].function} >>>>>")
        for dataframe in dataframes:
            try:
                print(tabulate(dataframe, headers="keys", tablefmt="psql"))
            except TypeError:
                print(dataframe, "\n")
        print("<<<<< Block has been calculated >>>>>")
        print("\n")

    def get_unique_users_in_test_groups(self):
        """
        Retrieves the unique users in the test groups
        and prints the count and percentage of users in each group.
        """
        self.print_df(
            self.df["test group"].value_counts(),
            self.df["test group"].value_counts(normalize=True),
            self.df.groupby("test group")["user id"].agg(["count", "nunique"]),
        )

    def get_unique_users_in_most_ads_hour(self):
        self.print_df(
            self.df.groupby("most ads hour")["user id"].agg(["count", "nunique"])
        )

    def get_stats(self):
        """
        Method to calculate statistics for the dataframe
        and print the grouped results.
        """
        group1 = self.df[self.df["test group"] == "ad"]
        group2 = self.df[self.df["test group"] == "psa"]

        self.df["total ads flag"] = (self.df["total ads"] > 0) * 1
        self.df["most ads hour flag"] = (self.df["most ads hour"] > 0) * 1
        self.print_df(
            self.df.groupby(["test group"])[["total ads flag", "total ads"]].agg(
                {"total ads flag": ["count", "mean"], "total ads": ["mean", "median"]}
            ),
            ttest_ind(a=group1["total ads"], b=group2["total ads"]),
            ttest_ind(a=group1["most ads hour"], b=group2["most ads hour"]),
        )

    def plot(self):
        self.df.groupby("most ads hour")["user id"].count().plot()
        pylab.show()

    def __len__(self):
        return len(self.df)


if __name__ == "__main__":
    df = pd.read_csv(CSV_FILE_PATH)
    df.head()
    ab = ABTesting(df)
    print(len(ab))

    ab.get_unique_users_in_test_groups()
    ab.get_unique_users_in_most_ads_hour()
    ab.get_stats()
    ab.plot()
