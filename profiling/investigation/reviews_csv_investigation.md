# 🔎 Reviews CSV Investigation

> **Status:** Ongoing  
> **Dataset:** `olist_order_reviews_dataset.csv`  
> **Purpose:** Investigate unexpected values discovered during data-quality profiling.

This investigation started as part of the normal data-profiling work on the reviews dataset.

---

## 1. 🟡 Initial NULL Profiling

The first step was the standard NULL check across the reviews table.

`review_id` was found to contain **1 NULL value**.

Instead of immediately treating this as a simple missing-ID problem, I inspected the affected record.

---

## 2. 🚨 First Anomaly Discovered

The affected row did not simply contain a missing `review_id`.

The values appeared to be **shifted into the wrong columns**. Customer review text was appearing in fields such as:

- `order_id`
- `review_score`

For example, text from the customer review appeared where an order ID and review score would normally be expected.

This suggested that there might be a problem with how some records were being parsed.

---

## 3. 🔍 Inspecting the Raw CSV

I then inspected the original CSV as raw text rather than relying only on the parsed Spark DataFrame.

A sample of the suspicious source data showed comma-separated values that appeared to use commas as the field delimiter.

At this point, the initial conclusion was that the problem might simply be a **malformed source record** rather than a broader parsing problem.

However, this conclusion was made **too early** because only a small number of raw records had been inspected.

---

## 4. ⚠️ Investigating the `review_score` Column

The schema showed that `review_score` had been read as a `string`.

Because `review_score` should contain numeric ratings from **1 to 5**, I checked for values that did not match the expected rating format.

### Result

| Check | Records identified |
|---|---:|
| Values failing the expected `1–5` review-score format | **2,558** |

At first, this appeared to capture the customer-review text that had been incorrectly placed into `review_score`, and it seemed that the problematic records might have been identified.

However, this check alone was **not sufficient**.

It only established that the values in `review_score` were not valid `1–5` scores. It did not explain the wider parsing problem or identify all affected records.

---

## 5. 🧩 Strengthening the Identifier Validation

The source IDs appeared to follow a **32-character hexadecimal format**.

A check based only on length was not sufficient because a value could contain exactly 32 characters without actually being a valid hexadecimal identifier.

For example, a 32-character string containing unrelated text could pass a length check.

The stronger validation therefore checked both:

- **Exactly 32 characters**
- **Every character being a valid hexadecimal character (`0–9` or `A–F`)**

When this stronger validation was applied to `review_id`, it identified:

> **4,937 records that did not conform to the expected format.**

This was significantly higher than the **2,558 records** previously identified through the `review_score` check.

| Investigation | Records identified |
|---|---:|
| Invalid `review_score` format | **2,558** |
| Invalid `review_id` hexadecimal format | **4,937** |

This suggested that the problem was much larger than the original single malformed record.

---

## 6. 🔬 Evidence of Wider Field Misalignment

The affected records were then visually inspected using the Databricks `display()` function.

The inspection showed that the problem was **not limited to customer review text appearing in `review_score`**.

The `review_id` field also contained values such as:

- **Timestamps**
- **Customer review text**
- **Other data that clearly belonged to different columns**

This indicated that the issue was affecting multiple fields and was therefore likely a broader **record-parsing or field-alignment problem**.

### Important distinction

The **4,937 records should not simply be described as 4,937 invalid IDs**.

The invalid ID format is evidence that the **parsed records are not always aligned with the intended CSV columns**.

---

## 7. 🔗 Identifier and Referential Integrity Checks

Additional validation was performed on the identifiers in the reviews dataset.

| Check | Records |
|---|---:|
| Invalid `order_id` hexadecimal format | **2,702** |
| `order_id` with no matching record in `orders` | **4,938** |
| Records with both an invalid `review_id` and an unmatched `order_id` | **4,937** |

The `order_id` format check identified **2,702 records** that do not conform to the expected 32-character hexadecimal format.

The referential integrity check identified **4,938 records** whose `order_id` does not exist in the `orders` table.

Most importantly, **4,937 records have both an invalid `review_id` and an unmatched `order_id`**.

This means that all 4,937 records identified by the `review_id` format check are also part of the unmatched `order_id` records.

The results provide stronger evidence that the anomalies affecting `review_id` and `order_id` are related to the same underlying field-alignment or parsing problem, rather than being independent ID-quality issues.

There is **one additional unmatched `order_id`** that does not fall within those 4,937 records. This should be investigated separately once the larger parsing issue has been understood.

---

## 8. 🔄 Revised Conclusion

The initial conclusion that the source CSV contained isolated malformed records was **too hasty**.

Inspecting a few raw records showed that commas were being used as delimiters and that some records appeared correctly structured. However, this was **not enough to establish that the entire file was being parsed correctly**.

The later identifier validation and inspection of thousands of affected records provided stronger evidence that the problem is broader than the single malformed record initially discovered.


### Current interpretation

The reviews dataset contains evidence of **widespread field misalignment during CSV ingestion/parsing**, rather than simply a small number of invalid IDs.

---

## 9. 🧭 Current Status

> **Investigation still in progress.**

The next step is to inspect the raw CSV more deeply and determine exactly why the affected records are becoming misaligned.

### Questions still to answer

- Is the source CSV itself malformed?
- Are quotation marks causing some records to be parsed incorrectly?
- Is there a specific pattern shared by the affected records?
- Can the affected records be recovered using different CSV-reader settings?
- If they cannot be reliably recovered, how should they be handled when moving from Bronze to Silver?

> **No records should be deleted or discarded at this stage.**

The goal is to identify the actual root cause before deciding how the data should be handled downstream.

---

## 📌 Key Finding So Far

What started as:

> **1 NULL `review_id`**

led to:

> **2,558 suspicious `review_score` values**

and then to:

> **4,937 `review_id` values that failed the expected hexadecimal format**

and further to:

> **2,702 `order_id` values that failed the expected hexadecimal format**

and:

> **4,938 `order_id` values with no matching record in `orders`**

Most importantly:

> **4,937 records have both an invalid `review_id` and an unmatched `order_id`.**

The investigation therefore moved from a simple NULL check to evidence of a potentially much broader **CSV field-alignment/parsing issue**.

**Root cause: not yet confirmed.**