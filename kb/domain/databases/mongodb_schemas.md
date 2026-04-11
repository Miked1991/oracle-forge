# MongoDB Schemas for DAB Datasets

## Yelp Dataset

## Collection: business (embedded reviews pattern)

```json
{
  "business_id": "abc123def456",
  "name": "String",
  "reviews": [
    {
      "review_id": "xyz789abc123",
      "user_id": "USER-12345",
      "stars": 4,
      "text": "Great place!",
      "date": "2025-07-15"
    }
  ]
}
```

## collection user

```json
{
  "user_id": "USER-12345",
  "name": "String",
  "review_count": 42,
  "average_stars": 4.2
}
```

## collection subscribers

```json
{
  "customer_id": "CUST-1234567",  // NOTE: String with prefix
  "plan_type": "postpaid",
  "monthly_revenue": 89.99
}
```

## collection support tickets

```json
{
  "ticket_id": "TKT-12345678",
  "customer_id": "CUST-1234567",  // Matches subscribers.customer_id
  "issue_description": "Frustrated with service",  // Unstructured
  "resolution_time_hours": 24
}
```

## collection claims

```json
{
  "claim_id": "CLM-98765",
  "patient_id": "PT-987654321",  // String with prefix
  "provider_npi": "NPI-1234567890",
  "clinical_notes": "Patient prescribed Lisinopril 10mg daily",
  "status": "paid"
}
```

## Critical Join Notes

All MongoDB IDs are STRINGS, may have prefixes

PostgreSQL IDs are INTs without prefixes

Always transform before joining

## Injection Test

Q: What is the format of customer_id in MongoDB telecom collection?
A: "CUST-1234567" (STRING with prefix)
