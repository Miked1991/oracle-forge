
---

## File: `kb/v2-domain/unstructured/text_extraction_patterns.md`

```markdown
# Unstructured Text Extraction Patterns for DAB

## Support Ticket Sentiment Extraction

**Field:** support_tickets.issue_description (TEXT) in MongoDB

**Extraction goal:** "negative sentiment" count

**Implementation:**

```python
negative_indicators = [
    'frustrated', 'angry', 'terrible', 'awful', 'worst',
    'broken', 'not working', 'failed', 'error', 'complaint',
    'unhappy', 'disappointed', 'useless', 'waste', 'terrible'
]

def extract_sentiment(text):
    text_lower = text.lower()
    is_negative = any(indicator in text_lower for indicator in negative_indicators)
    # Check for negation
    if 'not ' + indicator in text_lower:
        is_negative = False
    return 'negative' if is_negative else 'non-negative'
```

## Yelp Review Fact Extraction

Field: review.text (TEXT) in PostgreSQL

Extraction examples:

- "mentioned parking" → boolean

- "mentioned price" → boolean

## Implementation:

```python
def extract_mentioned_facilities(text):
    facilities = ['parking', 'wifi', 'outdoor seating', 'delivery', 'price']
    text_lower = text.lower()
    return [facility for facility in facilities if facility in text_lower]
```

## Healthcare Note Extraction

Field: clinical_notes (TEXT) in MongoDB

Extraction: medication names, dosages, frequencies

## Implementation

```python
medication_pattern = r'\b([A-Z][a-z]+)\s+(\d+\s*(mg|mcg|g|ml))\s+(daily|bid|tid|qid|weekly)\b'
matches = re.findall(medication_pattern, text)

# Returns: [('Lisinopril', '10 mg', 'daily'), ...]
```

## Critical Rule

Never return raw text when query asks for a count or structured fact.
Always apply extraction BEFORE counting or calculating.

## Injection Test

Q: How do I extract negative sentiment from support ticket text?
A: Use negative_indicators list with .lower() and any()
