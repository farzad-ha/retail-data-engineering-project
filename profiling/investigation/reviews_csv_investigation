Reviews CSV Investigation

This investigation started as part of the normal data-profiling work on the reviews dataset.

1. Initial NULL profiling

The first step was the standard NULL check across the reviews table.

review_id was found to contain 1 NULL value.

Instead of immediately treating this as a simple missing-ID problem, I inspected the affected record.

2. First anomaly discovered

The affected row did not simply contain a missing review_id.

The values appeared to be shifted into the wrong columns. Customer review text was appearing in fields such as order_id and review_score.

For example, text from the customer review appeared where an order ID and review score would normally be expected.

This suggested that there might be a problem with how some records were being parsed.

3. Inspecting the raw CSV

I then inspected the original CSV as raw text rather than relying only on the parsed Spark DataFrame.

A sample of the suspicious source data showed comma-separated values that appeared to use commas as the field delimiter.

At this point, the initial conclusion was that the problem might simply be a malformed source record rather than a broader parsing problem.

However, this conclusion was made too early because only a small number of raw records had been inspected.

4. Investigating the review_score column

The schema showed that review_score had been read as a string.

Because review_score should contain numeric ratings from 1 to 5, I checked for values that did not match the expected rating format.

This identified 2,558 records.

At first, this appeared to capture the customer-review text that had been incorrectly placed into review_score, and it seemed that the problematic records might have been identified.

However, this check alone was not sufficient.

It only established that the values in review_score were not valid 1–5 scores. It did not explain the wider parsing problem or identify all affected records.

5. Strengthening the identifier validation

The source IDs appeared to follow a 32-character hexadecimal format.

A check based only on length was not sufficient because a value could contain exactly 32 characters without actually being a valid hexadecimal identifier.

For example, a 32-character string containing unrelated text could pass a length check.

The stronger validation therefore checked both:

exactly 32 characters

every character being a valid hexadecimal character (0–9 or A–F)

When this stronger validation was applied to review_id, it identified 4,937 records that did not conform to the expected format.

This was significantly higher than the 2,558 records previously identified through the review_score check.

6. Evidence of wider field misalignment

The affected records were then visually inspected using the Databricks display() function.

The inspection showed that the problem was not limited to customer review text appearing in review_score.

The review_id field also contained values such as:

timestamps

customer review text

other data that clearly belonged to different columns

This indicated that the issue was affecting multiple fields and was therefore likely a broader record-parsing or field-alignment problem.

The 4,937 records should therefore not simply be described as 4,937 invalid IDs. The invalid ID format is evidence that the parsed records are not always aligned with the intended CSV columns.

7. Revised conclusion

The initial conclusion that the source CSV itself contained isolated malformed records was too hasty.

Inspecting a few raw records showed that commas were being used as delimiters and that some records appeared correctly structured, but that was not enough to establish that the entire file was being parsed correctly.

The later identifier validation and inspection of thousands of affected records provided stronger evidence that the problem is broader than the single malformed record initially discovered.

8. Current status

The investigation is not yet finished.

The next step is to inspect the raw CSV more deeply and determine exactly why the affected records are becoming misaligned.

The key questions are:

Is the source CSV itself malformed?

Are quotation marks causing some records to be parsed incorrectly?

Is there a specific pattern in the affected records?

Can the affected records be recovered with different CSV-reader settings?

If they cannot be reliably recovered, how should they be handled when moving from Bronze to Silver?

No records should be deleted or discarded at this stage.

The important finding so far is that the reviews dataset contains evidence of widespread field misalignment during CSV ingestion, rather than simply a small number of invalid IDs.