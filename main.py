"""
Main to try the functional of this simple project.
"""

from app.catalog import Catalog


# Create the catalog from test data
catalog = Catalog(
    product_path='test_data/products.csv',
    categories_path='test_data/categories.csv',
    product_category_links_path='test_data/links.csv'
)

# Test the functional
result = catalog.get_product_category_pairs()

# print result
result.show()
