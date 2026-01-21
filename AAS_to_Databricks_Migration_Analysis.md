# AAS to Databricks Migration Analysis Report

**Model:** Sales Model (Sales Cube)
**Analysis Date:** 2026-01-21
**Source File:** Sales Model.bim

---

## Model Overview

| Attribute | Value |
|-----------|-------|
| **Model Name** | Sales (Sales Cube) |
| **Compatibility Level** | 1500 (SSAS 2019/2022) |
| **Source Database** | Azure SQL - IKEA-DW (ikeasa.database.windows.net) |
| **Schema** | BV (Business View) |
| **Deployment Server** | asazure://westeurope.asazure.windows.net/ikea:rw |
| **Query Mode** | InMemory |

---

## Inventory Summary

| Component | Count |
|-----------|-------|
| **Tables** | ~145 |
| **Measures** | ~663 DAX expressions |
| **Calculated Columns** | ~70 |
| **Relationships** | ~354 |
| **CALCULATE() usages** | 267 |
| **SUMX/AVERAGEX/iterator functions** | 91 |
| **FILTER() usages** | 84 |
| **ALLSELECTED/ALLEXCEPT/context modifiers** | 36 |
| **SWITCH/RANKX/TOPN** | 4 |

---

## Table Categories

### Dimension Tables (~70 tables)
- Date (with Hijri calendar support)
- Time
- Agent Code
- Cashier
- Delivery Location
- Home Furnishing Business
- Item
- Product Area
- Product Range Area
- Order Status
- Order Type
- POS
- Sales Channel
- Sales Person
- Store
- Sell To Location
- Visit Source
- Currency
- ReportCurrency
- Document Type
- Payment Device Type
- Payment Type
- Payment Operation
- Refund Type
- Date Period
- Transaction Type
- Sales Location
- Merchant
- Payment Option
- Payment Merchant Type
- Service Provider
- Return Reason
- Post Code Distance Info
- OTIF Period Range
- And many more...

### Fact Tables (~45 tables)
- Sales (Main fact table)
- Visits
- Item Movement
- Item Price History
- Sales Budget
- Visit Budget
- Transaction Budget
- Exchange Rate
- Marketing Transactions
- Payment
- Fulfillment Flow
- LY_Sales
- LY_Visits
- And many more...

---

## Measure Complexity Classification

### 1. LOW Complexity (~40% of measures)
**Easily migrate to Databricks SQL - Simple aggregations, basic arithmetic**

| Example Measure | DAX Pattern | SQL Equivalent |
|-----------------|-------------|----------------|
| `Sales Amount` | `SUM(Sales[SLS_SALES_AMOUNT]) * [TrnExchangeRate]` | `SUM(amount) * exchange_rate` |
| `HFB Sales Quantity` | `SUM([SLS_HFB_SALES_QUANTITY])` | `SUM(quantity)` |
| `Transactions` | `DISTINCTCOUNT(Sales[Trans + Order])` | `COUNT(DISTINCT trans_order)` |
| `Cost Amount` | `SUM(Sales[SLS_COST_AMOUNT]) * [TrnExchangeRate]` | `SUM(cost) * rate` |
| `Average Check` | `DIVIDE([Sales Amount], [Transactions])` | `SUM(amount)/COUNT(DISTINCT trans)` |
| `ITEMS` | `SUM([SLS_SALES_QUANTITY])` | `SUM(quantity)` |
| `IFB Sales Amount` | `SUM(Sales[SLS_IFB_SALES_AMOUNT]) * [TrnExchangeRate]` | `SUM(ifb_amount) * rate` |
| `Service Sales Amount` | `SUM(Sales[SLS_SERVICES_SALES_AMOUNT]) * [TrnExchangeRate]` | `SUM(service_amount) * rate` |
| `Count of Orders` | `DISTINCTCOUNT([Order No])` | `COUNT(DISTINCT order_no)` |

**Recommendation:** Move to **Databricks SQL** as views or materialized views.

---

### 2. MEDIUM Complexity (~35% of measures)
**Migrate with effort - Uses CALCULATE with simple filters, YoY calculations, ratios**

| Example Measure | Complexity Factors |
|-----------------|-------------------|
| `Sales Amount.YoY%` | Ratio of current to LY measure |
| `Sales Amount.YoY` | Subtraction of LY from current |
| `Transactions.YoY%` | Ratio calculation |
| `Gross Margin 1` | `DIVIDE([Gross Profit 1], [Sales Amount])` - Division with multiple measure references |
| `Gross Profit 1` | `[Sales Amount] - [Total Cost]` - Measure subtraction |
| `Total Share Per Store` | `CALCULATE` with `ALL()` context modifiers |
| `ASIS Discount` | `SUMX(Sales, (...)) * [TrnExchangeRate]` - Iterator with `RELATED` lookup |
| `4 Week AVG` | `DATESINPERIOD` rolling windows |
| `8 Week AVG` | `DATESINPERIOD` rolling windows |
| `12 Week AVG` | `DATESINPERIOD` rolling windows |
| `HFB Rank` | `RANKX(ALL('Item'[HFB No Name]), Sales[Sales Amount])` |
| `CumulativeSumHFB` | `SUMX(TOPN([HFB Rank], ...), ...)` |
| `Index To Budget` | `DIVIDE([Sales Amount], [Budget.YT])` |
| `Total Sales Per 1000 Customers` | VAR with CALCULATE and ALL filter |

**Recommendation:** Can be **Databricks SQL** with window functions and CTEs, but requires careful translation of filter context.

---

### 3. HIGH Complexity (~25% of measures)
**Keep in Power BI or significant refactoring required**

| Measure | Complexity Details |
|---------|-------------------|
| `Sales Amount.LY` | 100+ line nested IF/CALCULATE with leap year handling, week/month/day logic |
| `ITEMS.LY` | Complex date shifting with fiscal year, calendar week matching |
| `Transactions.LY` | Dynamic period comparison with multiple filter conditions |
| `IFB Visits.LY` | Complex date shifting with fiscal year, calendar week matching |
| `Store Visits.LY` | Dynamic period comparison with multiple filter conditions |
| `ITEMS Per Transaction.LY` | References other LY measures |
| `Total Sales Per 1000 Customers.LY` | Complex nested CALCULATE with date filters |
| `ASIS Cost Amount.LY` | Multi-condition date comparison |
| `Total Share Per HFB and Item LY` | `ISFILTERED` checks, context-dependent branching |
| `Sales.Hijri.LY` | Hijri calendar-based YoY comparisons |
| `Daily Sales Share` | `ALLSELECTED` with conditional context based on slicer selections |
| `Iso PY` | Complex period matching logic |

#### Key Complexity Patterns Found:

1. **Leap Year Margin handling**
   ```dax
   VAR _MARGIN = IF(_Yr >= 2020 && _Mt * 100 + _Dy >= 228, 1, 0)
   ```

2. **Calendar Week matching**
   ```dax
   CONTAINS(VALUES('Date'[Calendar Week]), 'Date'[Calendar Week], 'Date'[Calendar Week])
   ```

3. **Fiscal Year comparisons**
   ```dax
   'Date'[Fiscal Year] = VALUES('Date'[Fiscal Year]) - 1
   ```

4. **Hijri calendar support**
   ```dax
   DT_HIJRI_DATENO = ('Date'[DT_HIJRI_YEAR]*10000)+('Date'[DT_HIJRI_MONTH]*100)+'Date'[DT_HIJRI_DAY]
   ```

5. **Dynamic filter detection**
   ```dax
   HASONEVALUE('Date'[Fiscal Year]) && NOT ISFILTERED('Date'[Month Name])
   ```

**Recommendation:** Keep in **Power BI** as these rely heavily on DAX filter context behavior that doesn't translate directly to SQL.

---

## Calculated Columns Assessment

| Type | Count | Complexity | Migration Path |
|------|-------|------------|----------------|
| Date calculations (WeekStartDate, DT_HIJRI_DATENO, Week Order 2) | ~15 | Low | Databricks - add to date dimension |
| Concatenations (HFB Display, Item No Name, HFB NO NAME) | ~20 | Low | Databricks - SQL CONCAT |
| Conditional (HFB Category) | ~10 | Medium | Databricks - CASE WHEN |
| Hierarchical (Top10 Items, Top10 Families, Top10 PA) | ~5 | Low | Databricks - string concatenation |
| Date extractions (EDS Month, EDS Year, EDS Month Name) | ~10 | Low | Databricks - MONTH(), YEAR(), FORMAT() |
| Complex (multi-condition IF chains) | ~10 | Medium-High | Evaluate case by case |

### Sample Calculated Column Conversions:

| DAX | SQL Equivalent |
|-----|----------------|
| `CONCATENATE('Item'[Item No] & " - ", 'Item'[Item Name EN])` | `CONCAT(item_no, ' - ', item_name_en)` |
| `DATEADD('Date'[Date], (-1 * 'Date'[Day of Week]) + 1, DAY)` | `DATE_ADD(date, -dayofweek(date) + 1)` |
| `MONTH('Item'[End Date Sales])` | `MONTH(end_date_sales)` |
| `FORMAT('Item'[End Date Sales], "MMMM")` | `DATE_FORMAT(end_date_sales, 'MMMM')` |

**Recommendation:** **Move all calculated columns to Databricks** as they're row-level calculations.

---

## Relationships Summary

- **Total Relationships:** 354
- **Standard one-to-many:** ~347
- **Bi-directional (cross-filtering):** 7

### Bi-directional Relationships (Require Special Handling):
1. Marketing Campaign Transaction Group ↔ Marketing Campaign
2. Marketing Customer ↔ Customers Balance
3. Date Period ↔ Date
4. Marketing Transactions ↔ Marketing Customer
5. Marketing Transactions ↔ Marketing Transaction Type
6. Marketing Transactions ↔ Merchant
7. Marketing Campaign Transaction Group ↔ Marketing Transactions

### Key Relationship Pattern:
All fact tables use surrogate keys (SRGT suffix) to join to dimensions:
- `DT_SRGT` → Date table
- `TM_SRGT` → Time table
- `STR_SRGT` → Store table
- `ITM_SRGT` → Item table
- `AGC_SRGT` → Agent Code table

---

## Migration Recommendations

### Move to Databricks SQL

1. **All base tables** - Direct SQL SELECT from BV schema
2. **All calculated columns** - Add to table definitions or views
3. **Simple measures** - SUM, COUNT, AVG, DISTINCTCOUNT
4. **Ratio calculations** - Division of simple measures
5. **Rolling windows** - Use SQL window functions (`AVG() OVER`)
6. **Rankings** - Use `RANK()` / `ROW_NUMBER()` window functions
7. **Static filters** - CALCULATE with fixed filter values

#### Sample Databricks SQL Pattern:

```sql
-- Sales Metrics View
CREATE OR REPLACE VIEW gold.vw_sales_metrics AS
SELECT
    d.fiscal_year,
    d.month_name,
    d.week,
    s.store_no_name,
    i.hfb_no_name,
    i.pa_no_name,

    -- Simple Measures
    SUM(f.sls_sales_amount) * er.factor AS sales_amount,
    SUM(f.sls_sales_quantity) AS items,
    COUNT(DISTINCT f.trans_order) AS transactions,
    SUM(f.sls_cost_amount) * er.factor AS cost_amount,
    SUM(f.sls_hfb_sales_amount) * er.factor AS hfb_sales_amount,
    SUM(f.sls_ifb_sales_amount) * er.factor AS ifb_sales_amount,

    -- Derived Measures
    SUM(f.sls_sales_amount) * er.factor / NULLIF(COUNT(DISTINCT f.trans_order), 0) AS average_check,
    SUM(f.sls_sales_quantity) / NULLIF(COUNT(DISTINCT f.trans_order), 0) AS items_per_transaction

FROM silver.fact_sales f
JOIN silver.dim_date d ON f.dt_date_srgt = d.dt_date_srgt
JOIN silver.dim_store s ON f.str_srgt = s.str_srgt
JOIN silver.dim_item i ON f.itm_srgt = i.itm_srgt
JOIN silver.exchange_rate er ON f.currency_code = er.source_currency
GROUP BY
    d.fiscal_year, d.month_name, d.week,
    s.store_no_name, i.hfb_no_name, i.pa_no_name, er.factor;
```

---

### Keep in Power BI

1. **Complex LY calculations** - All `.LY` measures with dynamic date logic
2. **Dynamic context measures** - Anything with `ISFILTERED`, `HASONEVALUE`
3. **Hijri calendar comparisons** - Keep as specialized DAX
4. **Leap year adjustments** - Custom date alignment logic
5. **Dynamic share calculations** - Context-aware ratios

### Estimated Split:
- **~60-65%** of measures can move to Databricks
- **~35-40%** should remain in Power BI

---

## Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATABRICKS                              │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │   BRONZE    │ →  │   SILVER    │ →  │        GOLD         │ │
│  │             │    │             │    │                     │ │
│  │ Raw Tables  │    │ Clean Views │    │ Pre-aggregated      │ │
│  │ from BV.*   │    │ Calculated  │    │ metrics             │ │
│  │             │    │ Columns     │    │                     │ │
│  │ - Date      │    │ - Date dim  │    │ - Daily aggregates  │ │
│  │ - Sales     │    │   with all  │    │ - Weekly aggregates │ │
│  │ - Item      │    │   calc cols │    │ - LY values in      │ │
│  │ - Store     │    │ - Fact with │    │   separate columns  │ │
│  │ - etc.      │    │   joined    │    │ - Pre-computed      │ │
│  │             │    │   dims      │    │   ratios            │ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ DirectQuery / Import
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         POWER BI                                │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Connect to Databricks Gold Layer                        │   │
│  │                                                         │   │
│  │ + Complex DAX measures only:                            │   │
│  │   - LY calculations with dynamic date logic             │   │
│  │   - Hijri calendar comparisons                          │   │
│  │   - Dynamic context measures (ISFILTERED, HASONEVALUE)  │   │
│  │   - Leap year adjustment logic                          │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Migration Effort Estimate

| Phase | Complexity | Notes |
|-------|------------|-------|
| Table migration to Databricks | Low | Straightforward lift from Azure SQL |
| Calculated column migration | Low | ~70 columns, mostly simple transformations |
| Simple measure conversion | Medium | ~260 measures to convert to SQL |
| Relationship recreation | Medium | 354 relationships to verify in views/joins |
| Complex measure analysis | High | ~230 measures need DAX expertise review |
| LY measure handling | High | Consider pre-computing in Databricks |
| Testing & validation | High | Extensive KPI validation required |

---

## Key Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| LY calculations accuracy | High | Create test cases for each date scenario (day, week, month, year) |
| Hijri calendar support | Medium | Validate with business users familiar with Islamic calendar |
| Bi-directional relationships | Medium | Replicate with SQL logic in views or handle in Power BI |
| Exchange rate application | Medium | Standardize pattern - currently used 50+ times |
| Performance degradation | Medium | Implement proper partitioning and clustering in Databricks |
| Filter context differences | High | Document all `CALCULATE` patterns and test thoroughly |

---

## Next Steps

1. **Inventory Validation** - Verify counts with business stakeholders
2. **Prioritize Measures** - Identify most-used measures for first migration wave
3. **Create Databricks Schema** - Design Bronze/Silver/Gold layers
4. **Develop Conversion Templates** - Standard patterns for DAX → SQL
5. **Build Test Framework** - Automated comparison between AAS and Databricks results
6. **Pilot Migration** - Start with one fact table (e.g., Sales) and its measures
7. **Iterate and Expand** - Add remaining tables and measures incrementally

---

## Appendix: Sample Complex Measure (For Reference)

### Sales Amount.LY (HIGH Complexity)
This measure demonstrates the complexity of LY calculations in this model:

```dax
Sales Amount.LY =
SUM(LY_Sales[SLS_SALES_AMOUNT]) * [TrnExchangeRate]

// Note: Actual implementation includes 100+ lines of commented code
// showing evolution of the logic handling:
// - Fiscal year boundaries
// - Calendar week alignment
// - Leap year margins
// - Day-of-week matching
// - Multiple filter scenarios (day, week, month, year)
```

The model currently uses a separate `LY_Sales` table for last year data, which simplifies the DAX but requires maintaining a parallel fact table.

---

*Generated by Claude Code - Migration Analysis Tool*
