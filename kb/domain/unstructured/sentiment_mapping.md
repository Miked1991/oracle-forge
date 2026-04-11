## File: `kb/domain/unstructured/sentiment_mapping.md`

```markdown
# Sentiment Mapping for DAB Unstructured Fields

## Complete Sentiment Lexicon

## **Negative Indicators (always check .lower()):**

frustrated, angry, terrible, awful, worst, broken, not working,
failed, error, complaint, unhappy, disappointed, useless, waste,
terrible, horrible, unacceptable, furious, annoyed, upset


## **Positive Indicators (for completeness):**

excellent, great, amazing, wonderful, happy, satisfied, perfect,outstanding, fantastic, helpful, quick, easy, recommended


## Negation Handling

If "not" precedes indicator, flip sentiment:
- "not good" → negative
- "not bad" → non-negative (not positive)

**Implementation:**

```python
def get_sentiment(text):
    text_lower = text.lower()
    
    # Check for negation
    if ' not ' in text_lower:
        for indicator in negative_indicators:
            if f'not {indicator}' in text_lower:
                return 'non-negative'
    
    # Standard check
    if any(indicator in text_lower for indicator in negative_indicators):
        return 'negative'
    return 'non-negative'
```
## Counting Negative Mentions

For query "count negative sentiment mentions":

Extract sentiment for each text field

Filter where sentiment == 'negative'

Count results

DO NOT: Return raw text and let user count
DO: Return integer count

## Injection Test

Q: How does negation affect sentiment classification?
A: "not good" is negative, "not bad" is non-negative
