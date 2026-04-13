"""Resolve ill-formatted join keys across different database systems."""

import re
from typing import Any, Dict, Callable, Optional, List
from functools import lru_cache

class JoinKeyResolver:
    """
    Normalize join keys across different formats and database systems.
    
    Handles:
    - Prefixes/suffixes (CUST_123, USER-456)
    - Different separators (PRD_123 vs PRD-123)
    - Type mismatches (string vs integer)
    - Case sensitivity
    - Whitespace issues
    - Partial matches (first N characters)
    """
    
    def __init__(self):
        # Registered normalization functions
        self._normalizers: Dict[str, Callable[[Any], Any]] = {}
        self._register_default_normalizers()
    
    def _register_default_normalizers(self):
        """Register built-in normalization strategies."""
        
        self._normalizers['strip_prefix'] = lambda x: re.sub(r'^[A-Z]+[_-]?', '', str(x))
        self._normalizers['strip_suffix'] = lambda x: re.sub(r'[_-]?[A-Z]+$', '', str(x))
        self._normalizers['normalize_separator'] = lambda x: re.sub(r'[_-]', '', str(x))
        self._normalizers['to_lower'] = lambda x: str(x).lower()
        self._normalizers['to_upper'] = lambda x: str(x).upper()
        self._normalizers['trim_whitespace'] = lambda x: str(x).strip()
        self._normalizers['extract_numbers'] = lambda x: re.sub(r'\D', '', str(x))
        self._normalizers['first_5_chars'] = lambda x: str(x)[:5].zfill(5)
        self._normalizers['last_5_chars'] = lambda x: str(x)[-5:]
    
    def resolve(
        self,
        key1: Any,
        key2: Any,
        strategy: Optional[str] = None,
        auto_detect: bool = True
    ) -> tuple[Any, Any]:
        """
        Apply normalization to both keys to enable matching.
        
        Args:
            key1: First key value
            key2: Second key value
            strategy: Specific normalization strategy to use
            auto_detect: Automatically detect required normalization
        
        Returns:
            Tuple of (normalized_key1, normalized_key2)
        """
        str_key1 = str(key1)
        str_key2 = str(key2)
        
        if auto_detect and not strategy:
            strategy = self.detect_strategy(str_key1, str_key2)
        
        if strategy and strategy in self._normalizers:
            norm_func = self._normalizers[strategy]
            return norm_func(key1), norm_func(key2)
        
        # Default: normalize separators and case
        return self._normalize_default(key1), self._normalize_default(key2)
    
    def detect_strategy(self, key1: str, key2: str) -> Optional[str]:
        """Automatically detect which normalization strategy is needed."""
        
        # Check for prefix mismatch (CUST_123 vs 123)
        if re.match(r'^[A-Z]+[_-]?\d+$', key1) and key2.isdigit():
            return 'strip_prefix'
        
        # Check for suffix mismatch
        if key1.isdigit() and re.match(r'^\d+[_-]?[A-Z]+$', key2):
            return 'strip_suffix'
        
        # Check for separator mismatch (PRD_123 vs PRD-123)
        if re.sub(r'[_-]', '', key1) == re.sub(r'[_-]', '', key2):
            return 'normalize_separator'
        
        # Check for case mismatch
        if key1.lower() == key2.lower():
            return 'to_lower'
        
        # Check for number extraction needed
        if re.sub(r'\D', '', key1) == re.sub(r'\D', '', key2):
            return 'extract_numbers'
        
        # Check for whitespace issues
        if key1.strip() == key2.strip():
            return 'trim_whitespace'
        
        return None
    
    def _normalize_default(self, value: Any) -> str:
        """Default normalization: lower, trim, remove separators."""
        s = str(value).lower().strip()
        s = re.sub(r'[_-]', '', s)
        return s
    
    @staticmethod
    @lru_cache(maxsize=1000)
    def can_join(key1: str, key2: str) -> bool:
        """Check if two keys can be joined after normalization.

        Static + lru_cache avoids the instance-method memory-leak where
        `self` would be kept alive indefinitely as a cache key.
        """
        norm1 = re.sub(r'[_-]', '', key1.lower().strip())
        norm2 = re.sub(r'[_-]', '', key2.lower().strip())
        return norm1 == norm2
    
    def resolve_cross_db_join(
        self,
        left_key: Any,
        right_key: Any,
        left_db_type: str,
        right_db_type: str
    ) -> tuple[Any, Any]:
        """
        Specialized resolution for cross-database joins.
        
        Handles common enterprise patterns:
        - PostgreSQL integer IDs vs MongoDB string IDs with prefix
        - SQLite TEXT vs DuckDB INTEGER
        """
        # PostgreSQL (int) ↔ MongoDB (CUST_123)
        if left_db_type == 'postgresql' and right_db_type == 'mongodb':
            if isinstance(left_key, int) and isinstance(right_key, str):
                # Extract number from MongoDB key
                extracted = re.sub(r'\D', '', right_key)
                if extracted and int(extracted) == left_key:
                    return left_key, int(extracted)
        
        # MongoDB ↔ PostgreSQL (reverse)
        if left_db_type == 'mongodb' and right_db_type == 'postgresql':
            if isinstance(left_key, str) and isinstance(right_key, int):
                extracted = re.sub(r'\D', '', left_key)
                if extracted and int(extracted) == right_key:
                    return int(extracted), right_key
        
        # SQLite TEXT ↔ DuckDB INTEGER
        if left_db_type in ('sqlite', 'duckdb') and right_db_type in ('sqlite', 'duckdb'):
            # Try numeric conversion
            try:
                left_num = int(re.sub(r'\D', '', str(left_key)))
                right_num = int(re.sub(r'\D', '', str(right_key)))
                if left_num == right_num:
                    return left_num, right_num
            except ValueError:
                pass
        
        # Default to standard resolution
        return self.resolve(left_key, right_key)

    def resolve_tcga_id(self, tcga_key: str) -> str:
        """Convert 'TCGA-AB-1234' → 'ab1234' to match PostgreSQL UUID format.

        Uses anchored prefix removal (^TCGA-) to avoid stripping embedded occurrences,
        then strips all remaining non-alphanumeric characters and lowercases.
        Required for PANCANCER_ATLAS cross-DB patient_id joins (M5).
        """
        without_prefix = re.sub(r'^TCGA-', '', tcga_key)
        return re.sub(r'[^A-Za-z0-9]', '', without_prefix).lower()

    def strip_cust_prefix(self, value: str) -> int:
        """'CUST-0001001' → 1001. Handles both CUST- and CUST_ delimiters.

        Uses a positive lookahead (?=\\d) so at least one digit is always kept,
        preventing ValueError on all-zero IDs like 'CUST-0000000' → 0.
        Required for crmarenapro CUST-prefix join key resolution (J4).
        """
        numeric = re.sub(r'^CUST[_-]0*(?=\d)', '', value)
        return int(numeric)

    def resolve_chain(self, key: Any, strategies: List[str]) -> Any:
        """
        Apply a sequence of normalization strategies in order.

        Example: resolve_chain('ID-98765', ['strip_prefix', 'first_5_chars']) -> '98765'
        The strip_prefix step must run before first_5_chars; calling first_5_chars alone
        on 'ID-98765' yields 'ID-98' (wrong). Required for M6.
        """
        result = key
        for strategy in strategies:
            if strategy in self._normalizers:
                result = self._normalizers[strategy](result)
        return result

    def resolve_pair_chain(
        self,
        key1: Any,
        key2: Any,
        strategies: List[str]
    ) -> tuple[Any, Any]:
        """Apply a chain of strategies to both keys."""
        return self.resolve_chain(key1, strategies), self.resolve_chain(key2, strategies)