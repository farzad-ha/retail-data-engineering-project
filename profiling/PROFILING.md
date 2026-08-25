# Data Quality Profiling Findings

> Initial profiling identified a small number of data-quality issues, with a deeper investigation required for the reviews CSV.

## Customers

- `customer_id` contains no NULLs and is unique.
- `customer_unique_id` can repeat, which is expected for customers with multiple orders.
- All `customer_id` values in `orders` matched a customer in `customers`.

## Orders

- No NULLs were found in the main identifiers.
- `order_status` contains 8 distinct statuses.
- Some delivery timestamps are missing.
- **8 orders** have `order_status = delivered` but no `order_delivered_customer_date`.
- One associated customer review mentions that the order had to be collected from a post office. This could potentially explain why a normal customer-delivery timestamp was not recorded, but the available data is not sufficient to confirm this as the reason.
- These 8 records are therefore retained as business-rule exceptions rather than being treated as invalid data.

## Order Items

- No NULLs were found.
- All `order_id` values in `order_items` matched an `order_id` in `orders`.
- All `product_id` values in `order_items` matched a `product_id` in `products`.
- `(order_id, order_item_id)` is unique and represents the expected row grain.

## Products

- `product_id` contains no NULLs and is unique.
- 610 products have NULL descriptive attributes.
- 2 products have missing physical-dimension attributes.
- The missing descriptive attributes do not currently affect products represented in `order_items`.

## Payments

- No NULLs were found.
- All `order_id` values in `payments` matched an `order_id` in `orders`.
- `payment_type = not_defined` occurs in 3 cancelled orders with zero payment value and is not currently considered an error.

## Reviews

A parsing issue was identified during profiling and resolved through a dedicated investigation. See `reviews_csv_investigation.md` for details.

## Geolocation

- No NULLs were found.
- Data types and sampled records appeared consistent.
- No obvious data-quality issues were identified.

## Sellers

- No NULLs were found.
- Data types and sampled records appeared consistent.
- Seller location fields overlap with the geolocation dataset but represent seller attributes rather than the geographic reference data.

## Product Category Translation

- No NULLs were found.
- The table provides Portuguese-to-English product category translations.
- The table is used as a reference/lookup dataset.

## Overall

The profiling covered:

- NULLs and completeness
- Data types
- Identifier format and uniqueness
- Table grain
- Referential integrity
- Useful categorical checks
- Business-rule inconsistencies
- Unexpected values

The main ingestion issue discovered during profiling was the **multi-line reviews CSV**, which was resolved by using an explicit schema with `multiLine = true`.