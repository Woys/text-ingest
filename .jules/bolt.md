## 2024-05-24 - Array over sorting for OpenAlex abstract reconstruction
**Learning:** In `OpenAlexFetcher._reconstruct_abstract`, parsing the inverted index using `sorted(position_word)` is significantly slower than allocating an array to `max_pos` and joining it.
**Action:** Use pre-allocated arrays where the index bounds are known or easily calculated, rather than dynamic dict allocation and sorting.
