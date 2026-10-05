%md
# Silver Layer Cleaning & Validation Plan

| Source CSV | Silver table | Main Silver cleaning / validation |
|---|---|---|
| `olist_customers_dataset.csv` | `silver.customers` | Trim/standardise text; validate `customer_id` format and uniqueness; validate expected grain; preserve `customer_unique_id` repeats. |
| `olist_orders_dataset.csv` | `silver.orders` | Standardise/validate timestamps; validate `order_status`; check logical timestamp order; flag the 8 delivered orders missing customer-delivery timestamp rather than automatically removing them. |
| `olist_order_items_dataset.csv` | `silver.order_items` | Validate (`order_id`, `order_item_id`) grain; validate IDs; validate `price` and `freight_value`; check relationships to orders, products, and sellers. |
| `olist_products_dataset.csv` | `silver.products` | Validate `product_id`; handle/standardise category and text fields; validate numeric dimensions; decide how NULL descriptive attributes should be represented downstream. |
| `olist_order_payments_dataset.csv` | `silver.payments` | Validate `payment_type`; validate installments and payment values; validate `order_id`; retain the 3 `not_defined` cancelled-order records because they appear valid in context. |
| `olist_order_reviews_dataset.csv` | `silver.reviews` | Ingest correctly with `multiLine=true`; validate IDs and `review_score`; validate timestamps; clean `review_comment_message` by using `review_comment_title` when the message is NULL; if both are NULL, use `"No comments provided"`; also treat `.`, `?`, `!`, `:`, `;`, `,` and empty strings as effectively empty comments; deduplicate valid rows by `review_id`; remove `review_comment_title` from Silver after it has been used as the fallback. |
| `olist_geolocation_dataset.csv` | `silver.geolocation` | Validate ZIP prefix; validate latitude/longitude ranges; standardise city/state text; remove exact duplicate records where all five meaningful geolocation fields are identical; retain different records that share the same ZIP prefix because multiple locations per ZIP prefix are legitimate. |
| `olist_sellers_dataset.csv` | `silver.sellers` | Validate `seller_id`; trim/standardise city/state; validate ZIP prefix; check expected grain. |
| `product_category_name_translation.csv` | `silver.product_category_name_translation` | Trim/standardise category names; validate uniqueness; ensure translation pairs are populated; keep as a reference/lookup table. |

**Key principle:** Bronze preserves the source. Silver makes the data trusted, standardised, validated, and ready for modelling.