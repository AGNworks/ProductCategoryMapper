"""
Module to work with products and categories.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


class Catalog:
    """
    Class to work with dataframes.
    """

    def __init__(
        self,
        product_path: str,
        categories_path: str,
        product_category_links_path: str
        ):
        self.session: SparkSession = SparkSession.builder.appName("ProductCategoryCSVExample").getOrCreate()

        self.products: DataFrame = self.load_dataframe(product_path)
        self.categories: DataFrame = self.load_dataframe(categories_path)
        self.product_category_links: DataFrame = self.load_dataframe(product_category_links_path)

    def load_dataframe(self, path: str) -> DataFrame:
        """
        Load the dataframe from file.
        """

        return self.session.read.csv(
            path,
            header=True,
            inferSchema=True  # automatically convert number-like strings to actual numeric types
        )


    def get_product_category_pairs(self) -> DataFrame:
        """
        Get all Product-category pair, and products without category.
        """

        # Join the products with its categories
        products_with_categories = (
            self.products.join(
                self.product_category_links,
                on="product_id",
                how="left"
            )
            .join(
                self.categories,
                on="category_id",
                how="left"
            )
        )

        # Select columns and create a new dataframe
        result = products_with_categories.select(
            col("product_name"),
            col("category_name")
        ).distinct().orderBy("product_name")

        return result
