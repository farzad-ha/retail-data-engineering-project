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

## 8. 🧪 Attempting Pattern-Based Record Reconstruction

Because the parsed records appeared to have values in the wrong columns, I explored whether the records could be reconstructed directly from the raw text without relying on the CSV delimiter structure.

The idea was to use the known characteristics of the fields as anchors:

- `review_id` → 32-character hexadecimal
- `order_id` → 32-character hexadecimal
- `review_score` → 1–5
- `review_creation_date` → timestamp
- `review_answer_timestamp` → timestamp

The two comment fields were temporarily treated as a single combined text field because they can contain arbitrary text and commas.

This produced a six-field reconstruction approach:

    review_id
    order_id
    review_score
    comments
    review_creation_date
    review_answer_timestamp

The Spark-based pattern matching identified **95,372** raw lines matching the expected structure, while **9,348** lines did not match.

However, visual inspection showed that this approach did not reliably reconstruct the intended records, so it was not used as the final solution.

The experiment was useful because it provided another way to investigate the raw data without relying on the normal CSV column parsing.

---

## 9. 🧪 Testing the CSV Ingestion Configuration

Several possible causes were tested before identifying the actual issue.

### Explicit Schema

The reviews dataset was read using an explicit seven-field schema:

    StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True)
    ])

The same anomalies remained:

- Invalid `review_id`: **4,937**
- Invalid `order_id`: **2,702**

This showed that schema inference was not the underlying cause.

### CSV Parsing Options

Additional CSV options were tested, including explicit separator, quote and escape settings.

These produced the same anomaly counts.

Therefore, changing the schema and the tested separator/quote settings did not resolve the issue.

---

## 10. ✅ Root Cause Identified

The reviews CSV contains **multi-line records**.

The file was initially being read without enabling multiline CSV support. As a result, physical lines belonging to the same logical review record could be interpreted incorrectly, causing values to appear shifted into the wrong columns.

The key ingestion option was:

    .option("multiLine", "true")

The corrected ingestion used both the explicit schema and multiline support:

    df_reviews_fixed = spark.read \
        .schema(schema) \
        .option("header", "true") \
        .option("multiLine", "true") \
        .csv("/Volumes/second_data_engineering_project/landing/raw_files/olist_order_reviews_dataset.csv")

---

## 11. 🔎 Validation After the Fix

The corrected DataFrame was re-profiled using the same checks that originally identified the problem.

All previously failing checks returned **0**.

| Validation | Before fix | After fix |
|---|---:|---:|
| Invalid `review_id` hexadecimal format | **4,937** | **0** |
| Invalid `order_id` hexadecimal format | **2,702** | **0** |
| Unmatched `order_id` in `orders` | **4,938** | **0** |
| Invalid `review_score` format | **2,558** | **0** |

This confirmed that the apparent field corruption was caused by the CSV ingestion configuration rather than thousands of independently corrupted identifiers.

---

## 📌 Final Conclusion

What started as:

> **1 NULL `review_id`**

led to:

> **2,558 suspicious `review_score` values**

then to:

> **4,937 invalid `review_id` values**

and:

> **4,938 unmatched `order_id` values**

The investigation initially suggested that the source records themselves might be malformed.

Further investigation ruled out schema inference and the tested separator/quote settings as the cause. A pattern-based reconstruction approach was also explored but was not reliable enough to use as the final solution.

The actual cause was **multi-line CSV records being read without `multiLine=true`**.

Once the reviews file was read using the explicit seven-field schema together with multiline support, the previously identified data-quality violations were resolved.

### Final Finding

> **Root cause confirmed: the reviews CSV contains multi-line records, and the initial ingestion configuration did not enable multiline CSV parsing.**

The source data was therefore not treated as thousands of independently corrupted IDs. The anomalies were caused by the way the records were being parsed during ingestion.

**Investigation: ✅ Resolved**