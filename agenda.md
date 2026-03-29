# 📋 Prompt vs Program Validation Agenda

## 🎯 Purpose

This document defines a comprehensive validation checklist to ensure: -
Prompt and implementation are fully aligned - No missing / extra /
incorrect logic exists - Output matches specification exactly

------------------------------------------------------------------------

## 🧭 How to Use

-   Treat this as a pre-submission checklist
-   Mark each item as:
    -   [x] Done
    -   [ ] Not Done
    -   \[N/A\] Not Applicable
-   Maintain traceability between Prompt → Code → Test Cases

------------------------------------------------------------------------

# ✅ 1. Requirement Coverage

-   [ ] All prompt requirements are implemented
-   [ ] No requirement is partially implemented
-   [ ] No requirement is missing

Example: Prompt: Validate ACTIVE passengers with CONFIRMED booking - \[
\] passenger_status = 'ACTIVE' - \[ \] booking_status = 'CONFIRMED'

------------------------------------------------------------------------

# ✅ 2. Requirement Interpretation

-   [ ] Business meaning correctly understood
-   [ ] No incorrect assumptions

Example: latest booking → MAX(booking_date)

------------------------------------------------------------------------

# ✅ 3. Input Validation

-   [ ] Input parameters validated
-   [ ] NULL input handled
-   [ ] Empty collections handled
-   [ ] Invalid values handled

------------------------------------------------------------------------

# ✅ 4. Output Contract Validation

-   [ ] Column order matches prompt
-   [ ] Format matches exactly
-   [ ] No extra characters/spaces

------------------------------------------------------------------------

# ✅ 5. SQL Logic Validation

-   [ ] JOIN conditions correct
-   [ ] No Cartesian product
-   [ ] No duplicate rows

------------------------------------------------------------------------

# ✅ 6. Filtering Logic

-   [ ] All filters applied
-   [ ] No missing WHERE conditions

------------------------------------------------------------------------

# ✅ 7. Aggregation Logic

-   [ ] GROUP BY correct
-   [ ] Aggregation matches requirement

------------------------------------------------------------------------

# ✅ 8. NULL Handling

-   [ ] Proper NULL checks used
-   [ ] NVL / COALESCE used where needed

------------------------------------------------------------------------

# ✅ 9. Exception Handling (PL/SQL)

-   [ ] NO_DATA_FOUND handled
-   [ ] TOO_MANY_ROWS handled
-   [ ] No invalid assumptions

------------------------------------------------------------------------

# ✅ 10. Control Flow Validation

-   [ ] All branches covered
-   [ ] No skipped logic paths

------------------------------------------------------------------------

# ✅ 11. Data Integrity

-   [ ] Correct columns used
-   [ ] Table relationships respected

------------------------------------------------------------------------

# ✅ 12. Business Rules

-   [ ] All business constraints enforced

------------------------------------------------------------------------

# ✅ 13. Edge Case Coverage

-   [ ] Empty dataset handled
-   [ ] Duplicate data handled
-   [ ] Boundary values handled

------------------------------------------------------------------------

# ✅ 14. Test Case Validation

-   [ ] Positive test cases present
-   [ ] Negative test cases present
-   [ ] Edge cases covered

------------------------------------------------------------------------

# ✅ 15. Output Exact Match

-   [ ] Case-sensitive match
-   [ ] Punctuation match
-   [ ] No extra spaces

------------------------------------------------------------------------

# ✅ 16. Unnecessary Logic Check

-   [ ] No redundant joins
-   [ ] No unused queries

------------------------------------------------------------------------

# ✅ 17. Assumption Validation

-   [ ] No hidden assumptions
-   [ ] Multi-row scenarios handled

------------------------------------------------------------------------

# ✅ 18. Transaction Handling

-   [ ] COMMIT used correctly
-   [ ] ROLLBACK on failure

------------------------------------------------------------------------

# ✅ 19. Performance Validation

-   [ ] No repeated queries inside loops
-   [ ] Bulk operations used where needed

------------------------------------------------------------------------

# ✅ 20. Code Completeness

-   [ ] No placeholders remain
-   [ ] No incomplete logic

------------------------------------------------------------------------

# ✅ 21. Traceability Matrix

  Requirement   Code Reference   Status
  ------------- ---------------- --------
  Example Req   line XX          \[ \]

------------------------------------------------------------------------

# ✅ 22. Validation Depth

-   [ ] Not just NULL checks
-   [ ] Valid range checks included

------------------------------------------------------------------------

# ✅ 23. Error Messages

-   [ ] Clear and meaningful
-   [ ] Matches expected wording

------------------------------------------------------------------------

# ✅ 24. Consistency Check

-   [ ] Naming consistent
-   [ ] Case consistent
-   [ ] Formatting consistent

------------------------------------------------------------------------

# 🚀 Final Gate

-   [ ] All requirements implemented
-   [ ] Output exact match
-   [ ] SQL logic correct
-   [ ] NULL + exceptions handled
-   [ ] Edge cases covered
-   [ ] Test cases aligned
-   [ ] No extra logic


# DB Configuration Reference
D:\Turing\Projects\workspace\app-validator\server\schema-db-config.mjs