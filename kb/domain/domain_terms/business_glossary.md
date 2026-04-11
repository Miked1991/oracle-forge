
---

## File: `kb/v2-domain/domain_terms/business_glossary.md`

```markdown
# Domain Term Definitions by Dataset

## Telecom Industry

| Term | Naive Interpretation | Correct Definition (for DAB) |
|------|---------------------|------------------------------|
| "active customer" | Has row in subscribers table | Purchased in last 90 days AND churn_date IS NULL |
| "churn" | Cancelled service | churn_date IS NOT NULL |
| "high-value customer" | High monthly_revenue | monthly_revenue > 100 AND plan_type = 'postpaid' |
| "fiscal quarter" | Calendar Q3 (Jul-Sep) | Telecom fiscal: Q3 = Oct-Dec |

## Retail (Yelp)

| Term | Naive Interpretation | Correct Definition |
|------|---------------------|-------------------|
| "popular business" | High review_count | review_count > 100 AND stars > 4.0 |
| "recent review" | Last 30 days | date > CURRENT_DATE - INTERVAL '30 days' |
| "power user" | High user.review_count | user.review_count > 50 AND user.fans > 10 |

## Healthcare

| Term | Naive Interpretation | Correct Definition |
|------|---------------------|-------------------|
| "readmission" | Same patient, same hospital | patient_id matches AND days_between < 30 |
| "out-of-network" | Provider not in network | provider_npi NOT IN network_list |
| "denied claim" | claim_status = 'denied' | status IN ('denied', 'rejected') |

## Where to Find Definitions

These definitions are NOT in database schemas.
Load this file before generating SQL for any query with ambiguous terms.

## Injection Test
Q: What does "active customer" mean in telecom?
A: Purchased in last 90 days AND churn_date IS NULL
