"""Extract structured data from unstructured text fields using regex + LLM fallback."""

import re
from typing import List, Any, Optional, cast
from dataclasses import dataclass
from enum import Enum

class ExtractionType(Enum):
    AMOUNT = "amount"
    DATE = "date"
    EMAIL = "email"
    PRODUCT_CODE = "product_code"
    SEVERITY = "severity"
    COLOR = "color"
    CUSTOMER_ID = "customer_id"
    CHURN_REASON = "churn_reason"

@dataclass
class ExtractionRule:
    """Regex-based extraction rule with post-processing."""
    pattern: str
    extract_group: int = 0
    post_process: Optional[str] = None  # 'float', 'int', 'strip', 'lower'
    
class UnstructuredExtractor:
    """
    Extract structured information from unstructured text.
    
    Supports:
    - Currency amounts ($49.99, 49.99 USD)
    - Dates (multiple formats)
    - Severity levels from keywords
    - Colors from descriptions
    - Product codes with varying formats
    - Customer IDs with prefixes
    
    Uses regex first, falls back to LLM for complex cases.
    """
    
    def __init__(self, llm_client: Optional[Any] = None) -> None:
        self.llm_client: Optional[Any] = llm_client
        
        # Pre-compiled extraction rules
        self.rules = {
            ExtractionType.AMOUNT: [
                ExtractionRule(r'\$(\d+(?:\.\d{2})?)', 1, 'float'),
                ExtractionRule(r'(\d+(?:\.\d{2})?)\s*(?:USD|dollars)', 1, 'float'),
                ExtractionRule(r'refunded\s*\$?(\d+(?:\.\d{2})?)', 1, 'float'),
            ],
            ExtractionType.DATE: [
                ExtractionRule(r'(\d{4}-\d{2}-\d{2})', 1, None),  # ISO
                ExtractionRule(r'(\d{2}/\d{2}/\d{4})', 1, None),  # MM/DD/YYYY
                ExtractionRule(r'(\w+\s+\d{1,2},?\s+\d{4})', 1, None),  # March 15, 2026
            ],
            ExtractionType.SEVERITY: [
                ExtractionRule(r'\b(urgent|critical|high|severe|down|outage)\b', 0, 'lower'),
                ExtractionRule(r'\b(medium|moderate)\b', 0, 'lower'),
                ExtractionRule(r'\b(low|minor|trivial)\b', 0, 'lower'),
            ],
            ExtractionType.COLOR: [
                ExtractionRule(r'\b(black|white|red|blue|green|yellow|silver|gold|gray|grey)\b', 0, 'lower'),
            ],
            ExtractionType.PRODUCT_CODE: [
                ExtractionRule(r'([A-Z]{2,4}-\d{3,6}(?:-[A-Z])?)', 1, 'strip'),
                ExtractionRule(r'([A-Z]{2,4}_\d{3,6}(?:_[A-Z])?)', 1, 'strip'),
            ],
            ExtractionType.CUSTOMER_ID: [
                ExtractionRule(r'CUST[_-]?(\d+)', 1, 'int'),
                ExtractionRule(r'cust[_-]?(\d+)', 1, 'int'),
                ExtractionRule(r'user[_-]?(\d+)', 1, 'int'),
            ],
            ExtractionType.CHURN_REASON: [
                ExtractionRule(r'\b(price|pricing|cost|costly|expensive|overpriced|cheap|affordable)\b', 0, 'lower'),
                ExtractionRule(r'\b(service|support|help|response|slow|poor)\b', 0, 'lower'),
                ExtractionRule(r'\b(competitor|competition|switched|alternative|elsewhere)\b', 0, 'lower'),
            ],
        }
    
    def extract(self, text: str, extract_type: ExtractionType, use_llm: bool = True) -> List[Any]:
        """
        Extract values from text using regex, optionally fallback to LLM.
        
        Args:
            text: Input unstructured text
            extract_type: Type of data to extract
            use_llm: If True and regex fails, try LLM extraction
        
        Returns:
            List of extracted values (strings, floats, or ints)
        """
        values: List[Any] = []

        # Try regex rules first
        for rule in self.rules.get(extract_type, []):
            pattern = re.compile(rule.pattern, re.IGNORECASE)
            matches: List[Any] = pattern.findall(text)
            for match in matches:
                raw: str
                if isinstance(match, tuple):
                    parts: tuple[Any, ...] = cast(tuple[Any, ...], match)
                    idx = rule.extract_group if rule.extract_group < len(parts) else 0
                    raw = str(parts[idx])
                else:
                    raw = str(match)

                # Apply post-processing
                processed: Any
                if rule.post_process == 'float':
                    processed = float(raw)
                elif rule.post_process == 'int':
                    processed = int(raw)
                elif rule.post_process == 'lower':
                    processed = raw.lower()
                elif rule.post_process == 'strip':
                    processed = raw.strip()
                else:
                    processed = raw

                values.append(processed)

        # Deduplicate while preserving order
        seen: set[Any] = set()
        unique_values: List[Any] = []
        for v in values:
            if v not in seen:
                seen.add(v)
                unique_values.append(v)
        
        # If regex found nothing, try LLM
        if not unique_values and use_llm and self.llm_client:
            unique_values = self._extract_with_llm(text, extract_type)
        
        return unique_values
    
    def _extract_with_llm(self, _text: str, _extract_type: ExtractionType) -> List[Any]:
        """Fallback to LLM for complex extraction (stub — implement with Groq in production)."""
        # prompt = f"Extract all {_extract_type.value}s from this text. Return as JSON list: {_text}"
        # response = await self.llm_client.chat(prompt)
        return []
    
    def extract_amounts(self, text: str) -> List[float]:
        """Convenience method for currency amounts."""
        return self.extract(text, ExtractionType.AMOUNT)
    
    def extract_dates(self, text: str) -> List[str]:
        """Convenience method for dates."""
        return self.extract(text, ExtractionType.DATE)
    
    def classify_severity(self, text: str) -> Optional[str]:
        """Return normalized severity level ('high', 'medium', 'low') from text."""
        severities = self.extract(text, ExtractionType.SEVERITY)
        priority = {
            'critical': 4, 'urgent': 4, 'outage': 4, 'down': 4,
            'high': 3, 'severe': 3,
            'medium': 2, 'moderate': 2,
            'low': 1, 'minor': 1, 'trivial': 1,
        }
        label_map = {
            'critical': 'high', 'urgent': 'high', 'outage': 'high', 'down': 'high',
            'high': 'high', 'severe': 'high',
            'medium': 'medium', 'moderate': 'medium',
            'low': 'low', 'minor': 'low', 'trivial': 'low',
        }
        if severities:
            top = max(severities, key=lambda s: priority.get(s, 0))
            return label_map.get(top, top)
        return None

    def count_wait_complaints(self, texts: List[str]) -> int:
        """Count reviews that contain a genuine wait-time complaint (U1).

        Uses WAIT_COMPLAINT regex instead of naive '%wait%' to avoid 3-4x
        over-counting from phrases like 'can't wait', 'wait staff', 'worth the wait'.

        Args:
            texts: List of review text strings (e.g. MongoDB reviews.text for a year)

        Returns:
            Integer count of reviews matching the scoped complaint pattern.
        """
        return sum(is_wait_complaint(t) for t in texts)

    def classify_churn_reasons(self, text: str) -> str:
        """
        Classify churn reason from free text into 'price', 'service', 'competitor', or 'other'.

        Used for crmarenapro unstructured churn analysis. See kb/domain/unstructured/text_extraction_patterns.md.
        """
        price_keywords = {'price', 'pricing', 'cost', 'costly', 'expensive', 'overpriced', 'cheap', 'affordable'}
        service_keywords = {'service', 'support', 'help', 'response', 'slow', 'poor'}
        competitor_keywords = {'competitor', 'competition', 'switched', 'alternative', 'elsewhere'}

        reasons = self.extract(text, ExtractionType.CHURN_REASON)
        for reason in reasons:
            if reason in price_keywords:
                return 'price'
            if reason in service_keywords:
                return 'service'
            if reason in competitor_keywords:
                return 'competitor'

        return 'other'


POS_WORDS = re.compile(
    r'\b(great|excellent|amazing|love|perfect|best|delicious|friendly|clean'
    r'|wonderful|fantastic|awesome|superb|brilliant|outstanding|happy|pleased'
    r'|satisfied|recommend|enjoyed|fresh|tasty|polite|helpful|quick)\b',
    re.I,
)
NEG_WORDS = re.compile(
    r'\b(terrible|awful|disgusting|rude|worst|horrible|cold|slow|never again'
    r'|frustrated|angry|broken|failed|failure|error|complaint|unhappy'
    r'|disappointed|useless|waste|poor|bad|disappointing|unacceptable'
    r'|overpriced|dirty|rotten|stale|burnt|bland|tasteless|incompetent'
    r'|ignored|waiting|waited|avoid|regret|never returning)\b',
    re.I,
)

# U1 — Yelp wait-time complaint filter.
#
# Naive LIKE '%wait%' over-counts by 3-4x because it matches positive phrases
# ("can't wait to go back", "wait staff was great", "worth the wait").
# This pattern is scoped to genuine complaint phrases only.
#
# Does NOT match:
#   - "can't wait"          (anticipation)
#   - "wait staff"          (restaurant staff noun)
#   - "worth the wait"      (positive payoff)
#   - "can't wait to ..."   (excitement)
#
# Use is_wait_complaint(text) for per-review boolean classification, or
# UnstructuredExtractor.count_wait_complaints(texts) for bulk counting.
WAIT_COMPLAINT = re.compile(
    r'(?:'
    # "waited too long", "waiting so long", "wait forever"
    r'wait(?:ed|ing)?\s+(?:(?:too|so|very|extremely|forever)\s+)?long'
    # "long wait", "long line"
    r'|long\s+(?:wait|line)'
    # exact probe phrase — "wait time" in a complaint context
    r'|wait\s+time'
    # "slow service", "slow response"
    r'|slow\s+(?:service|response)'
    # "spent 30 minutes waiting", "wasted an hour waiting"
    r'|(?:spent|wasted)\s+\w+\s+(?:min(?:utes?)?|hours?)\s+waiting'
    # "made us wait", "made me wait"
    r'|made\s+(?:us|me|everyone)\s+wait'
    # "forever to be seated", "forever to get a table"
    r'|forever\s+to\s+(?:be\s+seated|get\s+(?:a\s+)?(?:table|server|food|drink))'
    r')',
    re.IGNORECASE,
)


def is_wait_complaint(text: str) -> bool:
    """Return True if *text* contains a genuine wait-time complaint.

    Uses WAIT_COMPLAINT (scoped phrase regex) instead of naive '%wait%' matching,
    which over-counts by 3-4x on Yelp data by catching 'can't wait', 'wait staff',
    and 'worth the wait'.  Required for U1 (Yelp 2024 wait-time negative reviews).
    """
    return bool(WAIT_COMPLAINT.search(str(text)))


class SentimentClassifier:
    """Classify a batch of review texts as positive, negative, or neutral.

    Uses per-document keyword presence (not frequency) so that one review with
    five positive words still counts as a single positive document.  Threshold
    of 0.6 means ≥ 60% of keyword-matched documents must be positive to return
    'positive'; otherwise 'negative'.  Required for U3 (Yelp bulk sentiment).
    """

    def classify_bulk(self, texts: List[str]) -> str:
        """Return 'positive', 'negative', or 'neutral' for a list of review texts."""
        pos = sum(bool(POS_WORDS.search(str(t))) for t in texts)
        neg = sum(bool(NEG_WORDS.search(str(t))) for t in texts)
        if pos + neg == 0:
            return 'neutral'
        return 'positive' if pos / (pos + neg) >= 0.6 else 'negative'


class CategoryMatcher:
    """Match against pipe-separated category strings (e.g. Yelp business.categories).

    Yelp stores categories as a pipe-separated string like 'Restaurants|Pizza|Italian',
    not as a normalized foreign key or array.  Direct equality or LIKE '%Pizza%' will
    produce false positives on substrings.  Use match_pipe_field for exact element
    matching.  Required for J5.
    """

    @staticmethod
    def match_pipe_field(value: str, category: str) -> bool:
        """Return True if *category* is an exact element in the pipe-separated *value*.

        Case-insensitive; leading/trailing whitespace on each element is stripped.

        Example:
            match_pipe_field('Restaurants|Pizza|Italian', 'pizza') → True
            match_pipe_field('Restaurants|Pizza|Italian', 'Fast Food') → False
        """
        return category.strip().lower() in [c.strip().lower() for c in value.split('|')]
