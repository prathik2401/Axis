from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from axis.plugins.base import ChangeEvent
from utils.logging import get_logger

logger = get_logger(__name__)


class TransformationType(Enum):
    """Types of data transformations."""
    RENAME_COLUMN = "rename_column"
    CONVERT_TYPE = "convert_type"
    MASK_DATA = "mask_data"
    FILTER_COLUMN = "filter_column"
    COMPUTE_COLUMN = "compute_column"
    CUSTOM = "custom"


@dataclass
class TransformationRule:
    """Defines a transformation rule."""
    transformation_type: TransformationType
    table: Optional[str] = None  # None = applies to all tables
    column: Optional[str] = None
    target_column: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    custom_function: Optional[Callable[[Any], Any]] = None


class DataTransformer:
    """
    Transforms change events based on configurable rules.
    
    Use cases:
    - Column renaming for schema differences
    - Data type conversions
    - PII masking for compliance
    - Column filtering
    - Computed/derived columns
    """
    
    def __init__(self, rules: Optional[List[TransformationRule]] = None):
        """
        Initialize transformer with rules.
        
        Args:
            rules: List of transformation rules to apply
        """
        self.rules = rules or []
        self._rule_cache: Dict[str, List[TransformationRule]] = {}
        self._build_rule_cache()
    
    def add_rule(self, rule: TransformationRule) -> None:
        """
        Add a transformation rule.
        
        Args:
            rule: Transformation rule to add
        """
        self.rules.append(rule)
        self._build_rule_cache()
    
    def _build_rule_cache(self) -> None:
        """Build cache of rules by table for faster lookup."""
        self._rule_cache.clear()
        
        for rule in self.rules:
            table = rule.table or "*"  # * = all tables
            if table not in self._rule_cache:
                self._rule_cache[table] = []
            self._rule_cache[table].append(rule)
    
    def transform(self, event: ChangeEvent) -> ChangeEvent:
        """
        Transform a change event according to rules.
        
        Args:
            event: Original change event
            
        Returns:
            Transformed change event
        """
        # Get applicable rules
        table_rules = self._rule_cache.get(event.table, [])
        global_rules = self._rule_cache.get("*", [])
        applicable_rules = table_rules + global_rules
        
        if not applicable_rules:
            return event
        
        # Transform payload
        transformed_payload = event.payload.copy()
        transformed_old_values = event.old_values.copy() if event.old_values else None
        
        for rule in applicable_rules:
            try:
                if rule.transformation_type == TransformationType.RENAME_COLUMN:
                    transformed_payload = self._rename_column(
                        transformed_payload, rule
                    )
                    if transformed_old_values:
                        transformed_old_values = self._rename_column(
                            transformed_old_values, rule
                        )
                
                elif rule.transformation_type == TransformationType.CONVERT_TYPE:
                    transformed_payload = self._convert_type(
                        transformed_payload, rule
                    )
                    if transformed_old_values:
                        transformed_old_values = self._convert_type(
                            transformed_old_values, rule
                        )
                
                elif rule.transformation_type == TransformationType.MASK_DATA:
                    transformed_payload = self._mask_data(
                        transformed_payload, rule
                    )
                    if transformed_old_values:
                        transformed_old_values = self._mask_data(
                            transformed_old_values, rule
                        )
                
                elif rule.transformation_type == TransformationType.FILTER_COLUMN:
                    transformed_payload = self._filter_column(
                        transformed_payload, rule
                    )
                    if transformed_old_values:
                        transformed_old_values = self._filter_column(
                            transformed_old_values, rule
                        )
                
                elif rule.transformation_type == TransformationType.COMPUTE_COLUMN:
                    transformed_payload = self._compute_column(
                        transformed_payload, rule
                    )
                
                elif rule.transformation_type == TransformationType.CUSTOM:
                    if rule.custom_function:
                        transformed_payload = self._apply_custom(
                            transformed_payload, rule
                        )
                        if transformed_old_values:
                            transformed_old_values = self._apply_custom(
                                transformed_old_values, rule
                            )
                
            except Exception as e:
                logger.error(
                    f"Error applying transformation {rule.transformation_type.value} "
                    f"to column {rule.column}: {e}"
                )
        
        # Create new event with transformed data
        return ChangeEvent(
            table=event.table,
            schema=event.schema,
            operation=event.operation,
            payload=transformed_payload,
            old_values=transformed_old_values,
            timestamp=event.timestamp,
            primary_key=event.primary_key,
        )
    
    def _rename_column(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Rename a column."""
        if rule.column in data:
            value = data.pop(rule.column)
            data[rule.target_column or rule.column] = value
        return data
    
    def _convert_type(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Convert column data type."""
        if rule.column not in data:
            return data
        
        target_type = rule.parameters.get('type', 'str')
        
        try:
            if target_type == 'int':
                data[rule.column] = int(data[rule.column])
            elif target_type == 'float':
                data[rule.column] = float(data[rule.column])
            elif target_type == 'str':
                data[rule.column] = str(data[rule.column])
            elif target_type == 'bool':
                data[rule.column] = bool(data[rule.column])
            elif target_type == 'json':
                import json
                if isinstance(data[rule.column], str):
                    data[rule.column] = json.loads(data[rule.column])
        except (ValueError, TypeError) as e:
            logger.warning(
                f"Type conversion failed for {rule.column}: {e}"
            )
        
        return data
    
    def _mask_data(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Mask sensitive data."""
        if rule.column not in data:
            return data
        
        mask_type = rule.parameters.get('mask_type', 'full')
        
        value = data[rule.column]
        if value is None:
            return data
        
        value_str = str(value)
        
        if mask_type == 'full':
            # Full masking: ****
            data[rule.column] = '*' * len(value_str)
        
        elif mask_type == 'partial':
            # Partial masking: show first/last chars
            visible_chars = rule.parameters.get('visible_chars', 2)
            if len(value_str) > visible_chars * 2:
                data[rule.column] = (
                    value_str[:visible_chars] + 
                    '*' * (len(value_str) - visible_chars * 2) +
                    value_str[-visible_chars:]
                )
            else:
                data[rule.column] = '*' * len(value_str)
        
        elif mask_type == 'email':
            # Email masking: u***r@domain.com
            if '@' in value_str:
                local, domain = value_str.split('@', 1)
                if len(local) > 2:
                    masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
                else:
                    masked_local = '*' * len(local)
                data[rule.column] = f"{masked_local}@{domain}"
        
        elif mask_type == 'hash':
            # Hash the value
            import hashlib
            data[rule.column] = hashlib.sha256(
                value_str.encode()
            ).hexdigest()[:16]
        
        return data
    
    def _filter_column(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Remove a column from data."""
        if rule.column in data:
            data.pop(rule.column)
        return data
    
    def _compute_column(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Compute a new column from existing data."""
        expression = rule.parameters.get('expression')
        if not expression:
            return data
        
        try:
            # Simple expression evaluation
            # For safety, only allow specific columns
            result = eval(expression, {"__builtins__": {}}, data)
            data[rule.target_column or 'computed'] = result
        except Exception as e:
            logger.warning(f"Compute column failed: {e}")
        
        return data
    
    def _apply_custom(
        self, data: Dict[str, Any], rule: TransformationRule
    ) -> Dict[str, Any]:
        """Apply custom transformation function."""
        if rule.column in data and rule.custom_function:
            try:
                data[rule.column] = rule.custom_function(data[rule.column])
            except Exception as e:
                logger.error(f"Custom transformation failed: {e}")
        return data


# Predefined transformation helpers

def create_pii_masking_rules(tables: List[str]) -> List[TransformationRule]:
    """
    Create standard PII masking rules.
    
    Args:
        tables: List of tables to apply rules to
        
    Returns:
        List of transformation rules for common PII fields
    """
    rules = []
    
    pii_columns = {
        'email': ('partial', {'mask_type': 'email'}),
        'phone': ('partial', {'mask_type': 'partial', 'visible_chars': 3}),
        'ssn': ('full', {'mask_type': 'full'}),
        'credit_card': ('partial', {'mask_type': 'partial', 'visible_chars': 4}),
        'password': ('full', {'mask_type': 'hash'}),
    }
    
    for table in tables:
        for column, (mask_type, params) in pii_columns.items():
            rules.append(
                TransformationRule(
                    transformation_type=TransformationType.MASK_DATA,
                    table=table,
                    column=column,
                    parameters=params
                )
            )
    
    return rules


def create_column_filter_rules(
    tables: List[str], 
    columns_to_exclude: List[str]
) -> List[TransformationRule]:
    """
    Create rules to filter out specific columns.
    
    Args:
        tables: List of tables
        columns_to_exclude: Columns to remove
        
    Returns:
        List of filter transformation rules
    """
    rules = []
    
    for table in tables:
        for column in columns_to_exclude:
            rules.append(
                TransformationRule(
                    transformation_type=TransformationType.FILTER_COLUMN,
                    table=table,
                    column=column
                )
            )
    
    return rules
